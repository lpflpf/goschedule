package utils

import "net"

func GetInternalIp() string {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		//
	}
	for _, a := range addresses {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			return ipnet.IP.String()
		}
	}

	return ""
}

// set arr1 - arr2
func ArraySubtract(arr1, arr2 []string) []string {
	rMap := map[string]struct{}{}

	for _, key := range arr2 {
		rMap[key] = struct{}{}
	}

	var res []string

	for _, key := range arr1 {
		if _, ok := rMap[key]; !ok {
			res = append(res, key)
		}
	}
	return res
}
