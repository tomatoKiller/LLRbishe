// hello world
package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"go-simplejson"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	IP                   = ""
	PORT                 = 12345
	packetTypeFieldName  = "packetType"
	UserNetSpeed         = 200000 //  B/s
	JsonSplitString      = "msnserver"
	MaxUserNumber        = 5
	MaxGroupNumber       = 20
	MsgBoxLen            = 30
	MaxPkgNum            = 20
	InitailLoginInterval = 3600000
)

type pkg struct {
	throwId     int
	destination string
	size        int
}

type costOfUser struct {
	Value    float64
	Username string
}

type user struct {
	Conn            *net.TCPConn
	Username        string
	Password        string
	LastLoginTime   time.Time
	LastLogoutTime  time.Time
	LoginTime       time.Time
	LoginInterval   []int64 // 最近5次登录间隔时间
	Online          bool
	groups          map[string]*group
	MsgBox          [][]byte
	pkgMap          map[int]pkg
	onlineCostTable map[int][]costOfUser
	netSpeed        int
	friends         map[string]*user
	lock            *sync.RWMutex
}

type group struct {
	clients []*user
	name    string
	lock    *sync.RWMutex
}

type Server struct {
	clients           []*user
	ip                string
	port              int
	curOnlineUsersNum int
	configured        bool
	userNumber        int
	log               *os.File
	logger            *log.Logger
	lock              *sync.RWMutex
}

func (s *Server) LogInfo(format string, info ...interface{}) {

	s.logger.SetPrefix("info\t")
	s.logger.Printf(format, info...)
	fmt.Printf(format, info...)

}

func (s *Server) LogDebug(format string, info ...interface{}) {
	s.logger.SetPrefix("debug\t")
	s.logger.Printf(format, info...)

	fmt.Printf(format, info...)
}

func (s *Server) LogError(format string, info ...interface{}) {

	s.logger.SetPrefix("error\t")
	s.logger.Printf(format, info...)
	fmt.Printf(format, info...)

}

func (s *Server) LogFatal(format string, info ...interface{}) {

	s.logger.SetPrefix("Fatal\t")
	s.logger.Printf(format, info...)
	fmt.Printf(format, info...)

}

func (s *Server) Run() {
	if !s.configured {
		//此时日志还未进行配置
		fmt.Println("服务器尚未进行配置")
	}

	defer s.log.Close()

	listen, err := net.ListenTCP("tcp", &net.TCPAddr{net.ParseIP(s.ip), s.port, ""})

	if err != nil {
		s.LogFatal("监听端口失败: %s\n", err.Error())
	}

	s.LogInfo("%s\n", "服务器正常启动")

	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			s.LogFatal("接受客户端连接异常 %s\n", err.Error())
		}
		s.LogInfo("客户端连接来自: %s\n", conn.RemoteAddr().String())

		go s.handle(conn)
	}

}

func (s *Server) handle(conn *net.TCPConn) {

	data := make([]byte, 1024)
	username := ""

	for {
		i, err := conn.Read(data)

		if err != nil {
			if err == io.EOF {
				s.LogInfo("客户%s关闭连接\n", username)
			} else {
				s.LogInfo("读取客户%s连接错误\n", username)
				return
			}
			break
		}

		for _, rawReq := range strings.Split(string(data[0:i]), JsonSplitString) {
			fmt.Println("inner" + rawReq)
			reqBody, _ := simplejson.NewFromReader(bytes.NewReader([]byte(rawReq)))

			switch reqBody.Get(packetTypeFieldName).MustString() {
			case "login":
				s.handleLogin(conn, reqBody, &username)
				defer s.handleLogout(username)
			case "askOnlineCost":
				s.handleAskOnlineCost(reqBody, username)
			case "helpOnlineCost":
				s.handleHelpOnlineCost(reqBody, username)
			case "getFriends":
				s.handleGetFriends(reqBody, username)
			case "directSCF":
				s.handleDirectSCF(reqBody)
			case "forwardSCF":
				s.handleForwardSCF(reqBody)
			case "noticeAccept":
				s.handleNoticeAccept(reqBody, username)
			case "netSpeed":
				s.handleNetSpeed(reqBody, username)
			case "bufferedUp":
				s.handleBufferedUp(reqBody, username)
			case "logout":
				//				goto out
				return
			default:
				s.LogInfo("未知的报文类型: %s\n", reqBody.Get(packetTypeFieldName).MustString())
			}

		}
	}

}

//friends:   逗号分割的字符串
func (s *Server) createAndAddUser(username string, password string) *user {
	u := &user{}
	u.LoginTime = time.Now()
	u.LoginInterval = make([]int64, 5)
	for i := 0; i < len(u.LoginInterval); i++ {
		u.LoginInterval[i] = InitailLoginInterval
	}

	u.Online = false
	u.Username = username
	u.Password = password
	u.netSpeed = UserNetSpeed
	u.lock = new(sync.RWMutex)
	u.friends = make(map[string]*user, MaxUserNumber)
	u.groups = make(map[string]*group, MaxGroupNumber)
	u.pkgMap = make(map[int]pkg, MsgBoxLen)
	u.onlineCostTable = make(map[int][]costOfUser, MaxPkgNum)
	s.lock.Lock()
	s.clients = append(s.clients, u)
	s.lock.Unlock()
	return u
}

func (s *Server) handleLogin(conn *net.TCPConn, reqBody *simplejson.Json, username *string) {
	*username = reqBody.Get("account").MustString()
	password := reqBody.Get("password").MustString()

	u, err := s.findUser(*username)

	if err == nil {
		u.lock.Lock()
		u.Online = true
		u.LoginTime = time.Now()
		u.Conn = conn

		u.LoginInterval = append(u.LoginInterval[1:5], time.Now().Sub(u.LoginTime).Nanoseconds()/1000000)
		u.LoginTime = time.Now()

		u.lock.Unlock()
		s.LogInfo("用户登录%s\n", *username)
	} else {
		//新用户
		u = s.createAndAddUser(*username, password)
		u.Conn = conn
		u.Online = true
		s.LogInfo("新用户注册(第一次登录)：%s\n", *username)
	}

	json := simplejson.New()
	json.Set(packetTypeFieldName, "login")
	json.Set("response", "ok")
	response, _ := json.EncodePretty()
	u.lock.Lock()
	u.Conn.Write(response)

	s.LogInfo("发送登录反馈报文 to %s\n", *username)
	u.lock.Unlock()

	s.lock.Lock()
	s.lock.Unlock()

}

func (s *Server) handleLogout(username string) {

	u, err := s.findUser(username)

	if err != nil {
		s.LogError("用户%s不存在，无法执行logout\n", username)
	} else {
		s.LogInfo("用户%s退出\n", username)
		u.Online = false
		json := simplejson.New()
		json.Set(packetTypeFieldName, "logout")
		json.Set("response", "ok")
		response, _ := json.EncodePretty()
		time.Sleep(3 * time.Second)
		u.Conn.Write(response)
		u.Conn.CloseWrite()
		u.Conn = nil
	}

}

func (s *Server) handleAskOnlineCost(reqBody *simplejson.Json, username string) {
	s.LogInfo("接收到handleAskOnlineCost请求，来自于%s\n", username)
	throwID, _ := reqBody.Get("throwID").Int()
	des, _ := reqBody.Get("destination").String()
	pkgSize, _ := reqBody.Get("size").Int()

	s.lock.Lock()
	s.lock.Unlock()

	s.lock.Lock()

	s.curOnlineUsersNum = 1

	for i := 0; i < len(s.clients); i++ {
		if s.clients[i].Username != username && s.clients[i].Online {
			json := simplejson.New()
			json.Set(packetTypeFieldName, "helpOnlineCost")
			json.Set("throwID", throwID)
			json.Set("fromWho", username)
			json.Set("destination", des)
			json.Set("size", pkgSize)
			response, _ := json.EncodePretty()

			s.curOnlineUsersNum++
			s.clients[i].Conn.Write(response)
			s.LogInfo("发送helpOnlineCost报文 to %s\n", s.clients[i].Username)

		} else {
			s.clients[i].pkgMap[throwID] = pkg{throwID, des, pkgSize}
		}
	}
	s.lock.Unlock()

	u, _ := s.findUser(username)
	u.lock.Lock()
	u.onlineCostTable[throwID] = make([]costOfUser, 0, s.curOnlineUsersNum-1)

	if s.curOnlineUsersNum == 1 {
		json := simplejson.New()
		json.Set(packetTypeFieldName, "responseOnlineCost")
		json.Set("throwID", throwID)
		json.Set("destination", des)

		for i := 0; i < s.curOnlineUsersNum-1; i++ {
			u.onlineCostTable[throwID] = append(u.onlineCostTable[throwID], costOfUser{math.MaxFloat64, ""})
		}

		costOnline, retType, forwarder := s.costCalculate(des, u.pkgMap[throwID])

		json.Set("value", costOnline)
		json.Set("value", 1)
		json.Set("type", retType)
		json.Set("forwarder", forwarder)
		response, _ := json.EncodePretty()

		s.LogDebug("%s\n", response)

		u.Conn.Write(response)

	}

	u.lock.Unlock()

}

func (s *Server) costCalculate(destination string, p pkg) (float64, int, string) {
	effect := []float64{1 / 3.0, 4 / 15.0, 1 / 5.0, 2 / 15.0, 1 / 15.0}

	interval := float64(0)
	u, _ := s.findUser(destination)

	if !u.Online {
		for i := 0; i < 5; i++ {
			interval += float64(u.LoginInterval[i]) * effect[i]
		}

		interval -= float64(time.Now().Sub(u.LoginTime).Nanoseconds() / 1000000)
	}

	C := ""
	K := math.MaxFloat64

	for _, e := range u.onlineCostTable[p.throwId] {
		if e.Value < K {
			K = e.Value
			C = e.Username
		}
	}

	costOnline := float64(0) //代价值
	retType := 0             //包传递类型
	forwarder := ""          //中继者

	e1 := float64(p.size)/float64(u.netSpeed) + float64(interval)
	if e1 < K {
		costOnline = e1
		retType = 1

	} else {
		costOnline = K
		retType = 2
		forwarder = C
	}
	return costOnline, retType, forwarder

}

func (s *Server) handleHelpOnlineCost(reqBody *simplejson.Json, username string) {
	s.LogInfo("接收到handleHelpOnlineCost请求，来自于%s\n", username)
	throwID, _ := reqBody.Get("throwID").Int()
	toWho, _ := reqBody.Get("toWho").String()
	value, _ := reqBody.Get("value").Float64()

	u, err := s.findUser(toWho)
	if err != nil {
		s.LogFatal("handleHelpOnlineCost: 用户不存在: %s\n", username)
	}

	_, ok := u.onlineCostTable[throwID]
	if !ok {
		u.onlineCostTable[throwID] = make([]costOfUser, 0, s.userNumber-2)
	}
	u.onlineCostTable[throwID] = append(u.onlineCostTable[throwID], costOfUser{value, username})
	if len(u.onlineCostTable[throwID]) == cap(u.onlineCostTable[throwID]) {
		json := simplejson.New()
		json.Set(packetTypeFieldName, "responseOnlineCost")
		json.Set("throwID", throwID)
		json.Set("destination", u.pkgMap[throwID].destination)

		costOnline, retType, forwarder := s.costCalculate(username, u.pkgMap[throwID])
		json.Set("value", costOnline)
		json.Set("type", retType)
		json.Set("forwarder", forwarder)
		response, _ := json.EncodePretty()
		u.lock.Lock()
		u.Conn.Write(response)
		u.lock.Unlock()

	}

}

func (s *Server) handleForwardSCF(reqBody *simplejson.Json) {
	s.LogInfo("%s\n", "接收到ForwardSCF请求")
	forwarder, _ := reqBody.Get("forwarder").String()
	u, err := s.findUser(forwarder)
	if err != nil {
		s.LogFatal("handleForwardSCF: 用户不存在: %s\n", forwarder)
	}

	response, _ := reqBody.EncodePretty()
	u.lock.Lock()
	u.Conn.Write(response)
	u.lock.Unlock()
}

func (s *Server) findUser(username string) (*user, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for i := 0; i < len(s.clients); i++ {
		if s.clients[i].Username == username {
			return s.clients[i], nil
		}
	}

	return nil, errors.New("用户不存在")
}

func (s *Server) handleDirectSCF(reqBody *simplejson.Json) {
	s.LogInfo("%s\n", "接收到DirectSCF请求")
	destination, _ := reqBody.Get("destination").String()
	reqBody.Set("arriveTime", time.Now().Nanosecond()/1000000)
	response, _ := reqBody.EncodePretty()

	u, err := s.findUser(destination)
	if err != nil {
		s.LogFatal("handleDirectSCF: 用户不存在: %s\n", destination)

	}

	u.lock.Lock()

	if u.Online {
		u.Conn.Write(response)
		s.handleNoticeAccept(reqBody, destination)
	} else {
		u.MsgBox = append(u.MsgBox, response)
	}

	u.lock.Unlock()

}

func (s *Server) handleNoticeAccept(reqBody *simplejson.Json, username string) {
	s.LogInfo("接收到NoticeAccept请求，来自于 %s\n", username)
	arriveTime, _ := reqBody.Get("arriveTime").Int64()
	throwID, _ := reqBody.Get("throwID").Int()

	json := simplejson.New()
	json.Set(packetTypeFieldName, "noticeAccept")
	json.Set("arriveTime", arriveTime)
	json.Set("destination", username)
	json.Set("throwID", throwID)
	response, _ := json.EncodePretty()

	pass, _ := reqBody.Get("pass").String()

	for _, relayer := range strings.Split(pass, "+") {
		u, err := s.findUser(relayer)
		if err != nil {
			s.LogFatal("handleNoticeAccept: 用户不存在: %s\n", username)
		}
		u.lock.Lock()
		u.Conn.Write(response)
		u.lock.Unlock()
	}

	source, _ := reqBody.Get("source").String()
	u, err := s.findUser(source)
	if err != nil {
		s.LogFatal("handleNoticeAccept: 用户不存在: %s\n", username)
	}

	u.lock.Lock()
	u.Conn.Write(response)
	u.lock.Unlock()
}

func (s *Server) handleNetSpeed(reqBody *simplejson.Json, username string) {
	s.LogInfo("接收到NetSpeed请求，来自于 %s\n", username)

	u, err := s.findUser(username)
	if err != nil {
		s.LogFatal("handleNetSpeed: 用户不存在: %s\n", username)
	}

	//只有本用户会去读这个变量，不加锁
	u.netSpeed, _ = reqBody.Get("netSpeed").Int()

}

func (s *Server) handleBufferedUp(reqBody *simplejson.Json, username string) {
	s.LogInfo("接收到BufferedUp请求，来自于%s\n", username)
	throwID, _ := reqBody.Get("throwID").Int()
	destination, _ := reqBody.Get("destination").String()
	u, err := s.findUser(username)
	if err != nil {
		s.LogFatal("handleBufferedUp: 用户不存在: %s\n", username)
	}

	_, retType, forwarder := s.costCalculate(username, u.pkgMap[throwID])

	var dest *user
	if retType == 1 {
		dest, _ = s.findUser(destination)
	} else if retType == 2 {
		dest, _ = s.findUser(forwarder)
	} else {
		s.LogFatal("handleBufferedUp: 转发类型未知: \n", string(retType))

	}

	response, _ := reqBody.EncodePretty()
	dest.lock.Lock()
	dest.Conn.Write(response)
	dest.lock.Unlock()
	s.lock.Lock()
	s.lock.Unlock()
}

func (s *Server) handleGetFriends(reqBody *simplejson.Json, username string) {
	s.LogInfo("接收到GetFriends请求，来自于 %s\n", username)
	json := simplejson.New()
	json.Set(packetTypeFieldName, "Friends")

	groupsName := ""
	u, _ := s.findUser(username)
	counter := 0

	for _, g := range u.groups {
		groupsName += g.name

		groupUser := ""
		for j, u := range g.clients {
			groupUser += u.Username
			if j != len(g.clients)-1 {
				groupUser += "@"
			}
		}

		json.Set(g.name, groupUser)

		if counter != len(u.groups)-1 {
			groupsName += "@"
		}
		counter++
	}

	json.Set("groupsName", groupsName)

	response, _ := json.EncodePretty()
	u.lock.Lock()
	u.Conn.Write(response)

	time.Sleep(time.Second * 3)
	for j := 0; j < len(u.MsgBox); j++ {
		u.Conn.Write(u.MsgBox[j])
	}
	if len(u.MsgBox) != 0 {
		s.LogInfo("向%s发送缓存的数据包\n", u.Username)
		u.MsgBox = u.MsgBox[0:0]
	}

	u.lock.Unlock()

}
func (s *Server) Config(ip string, port int, logFile string, configDir string) {
	s.ip = ip
	s.port = port
	s.userNumber = MaxUserNumber
	s.clients = make([]*user, 0, MaxUserNumber)
	s.lock = new(sync.RWMutex)

	if f, err := os.Create(logFile); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	} else {
		//		s.log = f
		s.logger = log.New(f, "", log.LstdFlags|log.Lshortfile)
		s.LogInfo("%s\n", "日至系统配置完毕")
	}

	if st, err := os.Stat(configDir); err != nil {
		s.LogFatal("配置文件夹%s不存在\n", configDir)
	} else if !st.IsDir() {
		s.LogFatal("%s不是一个文件夹\n", configDir)
	}

	filepath.Walk(configDir, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		} else if f.IsDir() {
			return nil
		}

		if cfg, erro := os.Open(path); err != nil {
			s.LogFatal("打开配置文件%s失败\t%s\n", path, erro.Error())
		} else {
			bcfg := bufio.NewReader(cfg)
			keyword := ""
			line, linee := bcfg.ReadString('\n')
			for ; linee != io.EOF; line, linee = bcfg.ReadString('\n') {
				line = strings.TrimSpace(line)
				switch line {
				case "users":
					fallthrough
				case "groups":
					fallthrough
				case "friends":
					keyword = line
					continue
				}

				switch keyword {
				case "users":
					userinfo := strings.Split(line, "\t")
					s.createAndAddUser(userinfo[0], userinfo[1])
				case "groups":
					groupinfo := strings.Split(line, "\t")
					g := &group{}
					g.name = groupinfo[1]
					for _, gm := range strings.Split(groupinfo[2], ",") {
						if member, me := s.findUser(gm); me != nil {
							s.LogFatal("无法识别的group成员%s\n", gm)
						} else {
							g.clients = append(g.clients, member)
						}
					}

					if gowner, goe := s.findUser(groupinfo[0]); goe != nil {
						s.LogFatal("无法识别的group拥有者%s\n", groupinfo[0])
					} else {
						gowner.groups[g.name] = g
					}
				case "friends":
					friendspinfo := strings.Split(line, "\t")
					if u, ue := s.findUser(friendspinfo[0]); ue != nil {
						s.LogFatal("好友关系中无法识别的用户%s\n", friendspinfo[0])
					} else {
						for _, friname := range strings.Split(friendspinfo[1], ",") {
							if friuser, friuserE := s.findUser(friname); friuserE != nil {
								s.LogFatal("好友关系中无法识别的用户%s\n", friname)
							} else {
								u.friends[friuser.Username] = friuser
							}
						}
					}
				default:
					s.LogFatal("无法识别的配置文件类型%s\n", strings.TrimSpace(keyword))
				}
			}

		}
		return nil

	})

	s.lock.Lock()
	s.lock.Unlock()

	s.configured = true
	s.LogInfo("%s\n", "服务器配置完成")

}

func main() {
	server := new(Server)
	server.Config(IP, PORT, "log.txt", "config")
	server.Run()
	fmt.Println("done")
	fmt.Println("hsdjfdjf")
}
