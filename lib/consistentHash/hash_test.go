package consistentHash

import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"testing"
)

func TestHushValue(t *testing.T) {
	fmt.Println(murmur3.Sum32([]byte("asdfwasaf")))
}
