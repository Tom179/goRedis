package client

import (
	"net"
	"net/http"
	"time"
)

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

func MakeClient(addr string) (*http.Client, error) { //Client是哪个包
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	//return &http.Client{addr:}
}
