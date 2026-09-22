package server

import (
	"fmt"
	"im/internal/user"
	"io"
	"net"
	"strings"
	"sync"
)

type Server struct {
	Ip        string
	Port      string
	OnlineMap map[string]*user.User // 在线用户列表
	Message   chan string           // 广播信息通道
	MapLock   sync.RWMutex
}

func (this *Server) Start() error {
	// 1. 创建一个套接字
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", this.Ip, this.Port))
	if err != nil {
		fmt.Println("连接失败:", err)
		return err
	}

	// 2. 关闭套接子
	defer listener.Close()

	// 启动监听 ListenMessage
	go this.ListenMessage()

	// 3. 监听
	for {

		// 接受到连接
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("listener accept error:", err)
			continue
		}

		go this.handler(conn)

	}

}

func (this *Server) ListenMessage() {
	for {
		msg := <-this.Message // 获取消息

		// 上锁
		this.MapLock.Lock()
		for _, cli := range this.OnlineMap { //给每一个客户端发送信息
			cli.C <- msg
		}
		this.MapLock.Unlock()

	}
}

func (this *Server) Boradcast(user *user.User, msg string) {
	this.Message <- fmt.Sprintf("[%s] - %s: %s", user.Addr, user.Name, msg)
}

func (this *Server) handler(conn net.Conn) {

	user := user.NewUser(conn)

	// 上线业务
	this.online(user)

	// 处理用户接受消息
	go func() {

		buf := make([]byte, 4096) // 初始化一个 4k 的缓冲区

		for {
			n, err := conn.Read(buf) // 接受用户的信息
			if n == 0 {              // 下线
				this.offline(user)
				return
			}

			if err != nil && err != io.EOF { // 有错且不是结束符号
				fmt.Println("cocnnection error:", err)
				return
			}

			msg := string(buf[:n-1]) // 去除结尾的换行符
			this.handleMessage(user, msg)
		}

	}()

	// 阻塞当前handle
	select {}

}

// -------------- 处理用户业务

func (this *Server) online(u *user.User) {
	this.MapLock.Lock()
	this.OnlineMap[u.Name] = u
	this.MapLock.Unlock()

	this.Boradcast(u, "已上线")
}

func (this *Server) offline(u *user.User) {
	this.MapLock.Lock()
	delete(this.OnlineMap, u.Name)
	this.MapLock.Unlock()

	this.Boradcast(u, "已下线")
}

func (this *Server) handleMessage(u *user.User, msg string) {

	parts := strings.SplitN(msg, "|", 2) // 解析命令
	cmd := parts[0]

	switch cmd {

	// 查询在线人数
	case "who":
		this.MapLock.Lock()
		count := len(this.OnlineMap)
		this.MapLock.Unlock()
		u.WriteMessage(fmt.Sprintf("当前在线人数: %d人", count))

	// 修改名字业务
	case "rename":

		if len(parts) < 2 || parts[1] == "" { // 检查是否有效
			u.WriteMessage("用法: rename|新名字")
			return
		}

		this.MapLock.Lock()
		delete(this.OnlineMap, u.Name)
		this.OnlineMap[parts[1]] = u
		this.MapLock.Unlock()

		u.Name = parts[1]
		u.WriteMessage(fmt.Sprintf("您已更新用户名: %s", u.Name))

	// 默认发送信息
	default:
		this.Boradcast(u, msg)
	}
}

// 构造函数
func NewServer(ip string, port string) *Server {
	serever := &Server{
		Ip:        ip,
		Port:      port,
		Message:   make(chan string),
		OnlineMap: make(map[string]*user.User),
	}

	return serever
}
