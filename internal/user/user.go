package user

import (
	"net"
)

// 用户类
type User struct {
	Name string
	Addr string
	C    chan string
	conn net.Conn
}

func (this *User) ListenMessage() {
	// 监听信息, 当 channal 收到广播时 立刻返回给客户端

	for {
		msg := <-this.C                     // 阻塞等待新信息
		this.conn.Write([]byte(msg + "\n")) // 发送信息给客户端
	}
}

func (this *User) WriteMessage(msg string) {
	this.conn.Write([]byte(msg + "\n"))
}

// 构造函数
func NewUser(conn net.Conn) (user *User) {

	userAddr := conn.RemoteAddr().String()

	user = &User{
		Name: userAddr,
		Addr: userAddr,
		C:    make(chan string),
		conn: conn,
	}
	go user.ListenMessage()

	return

}
