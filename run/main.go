package main

import (
	"fmt"
	"net"
	"os"

	"github.com/tgruben/pgsimple"
)

const (
	CONN_HOST = "localhost"
	CONN_PORT = "55432"
	CONN_TYPE = "tcp"
)

func main() {
	// Listen for incoming connections.
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}
func handleRequest(client net.Conn) error {
	//client := *bufio.NewReader(socket)
	pgclient := pgsimple.NewHandler(client)
	if err := pgclient.Startup(); err != nil {
		return err
	}
	fmt.Println("READING PACKET")
	tp, packet, err := pgclient.ReadPacket()
	if err != nil {
		return err
	}
	pgsimple.Decode(tp, packet)
	for tp != pgsimple.Terminate {
		if tp == 0 {
			break
		}
		if tp == pgsimple.Query {
			fmt.Println("DO QUERY", packet)
			rs := pgsimple.BuildThowAwayResult()
			pgclient.Send(rs)
		}

		tp, packet, err = pgclient.ReadPacket()
		pgsimple.Decode(tp, packet)
	}
	fmt.Println("DONE\n\n")
	pgclient.Shutdown()
	return nil
}
