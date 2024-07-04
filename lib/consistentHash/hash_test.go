package consistentHash

import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"testing"
)

func TestHushValue(t *testing.T) {
	fmt.Println(murmur3.Sum32([]byte("127.0.0.1:6379")))
	//fmt.Println(crc32.ChecksumIEEE([]byte("127.0.0.1:6379")))
}
