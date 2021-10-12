package common

import (
	"crypto/md5"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func GetOdpEnv() string {
	env := ""
	env1 := os.Getenv("ODP_ENV")
	env2 := os.Getenv("env")
	if env1 != "" {
		env = env1
	} else {
		env = env2
	}

	return env
}

// param valid check
func CheckSign(sign, source, t string, ibiz int) bool {
	tmpStr := fmt.Sprintf("%s%s%d%s", source, source, ibiz, t)
	tmpSign := fmt.Sprintf("%x", md5.Sum([]byte(tmpStr)))

	if tmpSign == sign {
		return true
	}

	return false
}

func ParseStringToInterface(keyword string) []interface{} {
	var tmp []interface{}
	if keyword == "" {
		return tmp
	}
	s := strings.Split(keyword, ",")
	for _, v := range s {
		tmp = append(tmp, v)
	}
	return tmp
}

func InterfaceToString(it interface{}) string {
	var l interface{}

	switch it.(type) {
	case []interface{}:
		l = it.([]interface{})[0]
	default:
		l = it
	}

	switch l.(type) {
	case int:
		return strconv.Itoa(l.(int))
	case float64:
		return strconv.Itoa(int(l.(float64)))
	case string:
		return l.(string)
	}
	return ""
}

func GetWeightInfo(id string, info string) int {
	if id == "" {
		return 0
	}
	parts := strings.Split(info, ",")
	if len(parts) == 0 {
		return 0
	}

	for _, weightInfo := range parts {
		subs := strings.Split(weightInfo, "|")
		if len(subs) != 2 {
			continue
		}
		if subs[0] == id {
			weight, _ := strconv.Atoi(subs[1])
			return weight
		}
	}

	return 0
}

func CheckValidTime(id string, info string) bool {
	if id == "" {
		return false
	}

	parts := strings.Split(info, ",")
	if len(parts) == 0 {
		return false
	}

	for _, item := range parts {
		subs := strings.Split(item, "|")
		if len(subs) != 3 {
			continue
		}
		if subs[0] == id {
			stime, _ := strconv.ParseInt(subs[1], 10, 64)
			etime, _ := strconv.ParseInt(subs[2], 10, 64)
			now := time.Now().Unix()
			if now >= stime && now <= etime {
				return true
			}
		}
	}

	return false
}

func SortItems(items []map[string]interface{}, field string) {
	length := len(items)
	for i := 0; i < length-1; i++ {
		for j := i + 1; j < length; j++ {
			a := InterfaceToInt(items[i][field])
			b := InterfaceToInt(items[j][field])
			if a < b {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
}

func InterfaceToInt(it interface{}) int {
	var l interface{}

	switch it.(type) {
	case []interface{}:
		l = it.([]interface{})[0]
	default:
		l = it
	}

	switch l.(type) {
	case int:
		return l.(int)
	case float64:
		return int(l.(float64))
	case string:
		tmp, err := strconv.Atoi(l.(string))
		if err != nil {
			return 0
		} else {
			return tmp
		}
	}
	return 0
}
