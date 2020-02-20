package wrpc

import (
	"net"
	"time"
	"math/rand"
    "reflect"
    "runtime"
    "strings"
)

/*
shuffle
*/
func Shuffle(slice []interface{}) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for len(slice) > 0 {
		n := len(slice)
		randIndex := r.Intn(n)
		slice[n-1], slice[randIndex] = slice[randIndex], slice[n-1]
		slice = slice[:n-1]
	}
}

// get local ip
func GetLocalIp() string {
    addrs, err := net.InterfaceAddrs()
    if err != nil{
        return ""
    }
    ips := make([]string, 0)
    for _, value := range addrs{
        if ipnet, ok := value.(*net.IPNet); ok && !ipnet.IP.IsLoopback(){
            if ipnet.IP.To4() != nil{
                ips = append(ips, ipnet.IP.String())
            }
        }
    }
    if len(ips) > 0 {
    	    return ips[0]
    }
    return ""
}

/*
获取函数对象的名称
*/
func GetFuncName(f interface{}, seps ...rune) string {
    // 获取函数名称
    fn := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
    // 用 seps 进行分割
    fields := strings.FieldsFunc(fn, func(sep rune) bool {
        for _, s := range seps {
            if sep == s {
                return true
            }
        }
        return false
    })

    if size := len(fields); size > 0 {
        return fields[size-1]
    }
    return ""
}
