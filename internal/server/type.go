package server

import (
	"fmt"
	"net"
)

type Server struct {
	Ip   string
	Port string
}

func (s Server) Start() error {
	// 1. 创建一个套接字
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.Ip, s.Port))
	if err != nil {
		fmt.Println("连接失败:", err)
		return err
	}

	// 2. 关闭套接子
	defer listener.Close()

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

func (s *Server) handler(conn net.Conn) {

	fmt.Println("连接成功")

}

// 构造函数
func NewServer(ip string, port string) *Server {

	return &Server{
		Ip:   ip,
		Port: port,
	}

}
