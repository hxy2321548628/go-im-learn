package server

import (
	"fmt"
	"im/internal/user"
	"io"
	"net"
	"sync"
)

type Server struct {
	Ip        string
	Port      string
	OnlineMap map[string]*user.User // 在线用户列表
	Message   chan string           // 广播信息通道
	mapLock   sync.RWMutex
}

func (s *Server) Start() error {
	// 1. 创建一个套接字
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.Ip, s.Port))
	if err != nil {
		fmt.Println("连接失败:", err)
		return err
	}

	// 2. 关闭套接子
	defer listener.Close()

	// 启动监听 ListenMessage
	go s.ListenMessage()

	// 3. 监听
	for {

		// 接受到连接
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("listener accept error:", err)
			continue
		}

		go s.handler(conn)

	}

}

func (s *Server) ListenMessage() {
	for {
		msg := <-s.Message // 获取消息

		// 上锁
		s.mapLock.Lock()
		for _, cli := range s.OnlineMap { //给每一个客户端发送信息
			cli.C <- msg
		}
		s.mapLock.Unlock()

	}
}

func (s *Server) Boradcast(user *user.User, msg string) {
	s.Message <- fmt.Sprintf("[%s] - %s: %s", user.Addr, user.Name, msg)
}

func (s *Server) handler(conn net.Conn) {

	user := user.NewUser(conn)

	// 记录用户
	s.mapLock.Lock()              // 上锁, 防止并发修改map
	s.OnlineMap[user.Name] = user // 添加用户
	s.mapLock.Unlock()            // 解锁

	// 广播上线
	s.Boradcast(user, "已上线")

	// 处理用户接受消息
	go func() {

		buf := make([]byte, 4096) // 初始化一个 4k 的缓冲区

		for {
			n, err := conn.Read(buf) // 接受用户的信息
			if n == 0 {              // 下线
				s.Boradcast(user, "已下线")
				return
			}

			if err != nil && err != io.EOF { // 有错且不是结束符号
				fmt.Println("cocnnection error:", err)
				return
			}

			msg := string(buf[:n-1]) // 去除结尾的换行符
			s.Boradcast(user, msg)   // 将得到的信息广播
		}

	}()

	// 阻塞当前handle
	select {}

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
