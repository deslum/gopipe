package main

// #cgo pkg-config: python3
// #define Py_LIMITED_API
// #include <Python.h>
// int PyArg_ParseTuple_String(PyObject *, char**, char**, char**, char**);
// int PyArg_ParseTuple_Connection(PyObject *, char**, long long *);
// int PyArg_ParseTuple_Hashmap_Get_String(PyObject *, char**, char **);
// int PyArg_ParseTuple_Hashmap_Set_String(PyObject *, char**, char **, char **);
// int PyArg_ParseTuple_LL(PyObject *, long long *);
// PyObject* Py_String(char *pystring);
import (
	"C"
)
import (
	"fmt"
	"log"

	"github.com/go-redis/redis"
)

var client *redis.Client
var pipe redis.Pipeliner
var execChan chan []string

//export Connect
func Connect(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var ip *C.char
	var port C.longlong
	if C.PyArg_ParseTuple_Connection(args, &ip, &port) == 0 {
		log.Println(0)
		return C.PyLong_FromLong(0)
	}
	ipStr := C.GoString(ip)
	client = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", ipStr, port),
		Password: "", // no password set
		DB:       0,  // use default DB
		PoolSize: 10,
	})

	execChan = make(chan []string, 10)

	_, err := client.Ping().Result()
	if err != nil {
		log.Panicln(err)
		return C.PyLong_FromLong(0)
	}

	pipe = client.Pipeline()
	return C.PyLong_FromLong(0)
}

//export add_command
func add_command(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var cmd, hashmap, key, value *C.char
	if C.PyArg_ParseTuple_String(args, &cmd, &hashmap, &key, &value) == 0 {
		return C.PyLong_FromLong(0)
	}
	pipe.HSet(C.GoString(hashmap), C.GoString(key), C.GoString(value))
	return C.PyLong_FromLong(0)
}

//export execute
func execute(self *C.PyObject, args *C.PyObject) *C.PyObject {
	_, err := pipe.Exec()
	if err != nil {
		log.Println(err)
		return C.PyLong_FromLong(0)
	}
	err = pipe.Discard()
	if err != nil {
		log.Println(err)
	}
	return C.PyLong_FromLong(0)
}

//export hget
func hget(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var hashmap, key, value *C.char
	if C.PyArg_ParseTuple_Hashmap_Get_String(args, &hashmap, &key) == 0 {
		return C.PyLong_FromLong(0)
	}

	hashmapStr := C.GoString(hashmap)
	keyStr := C.GoString(key)
	_ = C.GoString(value)
	result := client.HGet(hashmapStr, keyStr)
	if result.Err() != nil {
		return C.PyLong_FromLong(0)
	}
	return C.PyLong_FromLong(0)
}

//export hset
func hset(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var hashmap, key, value *C.char
	if C.PyArg_ParseTuple_Hashmap_Set_String(args, &hashmap, &key, &value) == 0 {
		return C.PyLong_FromLong(0)
	}
	hashmapStr := C.GoString(hashmap)
	keyStr := C.GoString(key)
	valueStr := C.GoString(value)
	result := client.HSet(hashmapStr, keyStr, valueStr)
	if result.Err() != nil {
		return C.PyLong_FromLong(0)
	}
	return C.PyLong_FromLong(0)
}

func exec(bufNum int) {
	for {
		select {
		case ex, ok := <-execChan:
			if !ok {
				break
			}
			switch len(ex) {
			case 2:
				client.HGet(ex[0], ex[1])
			case 3:
				client.HSet(ex[0], ex[1], ex[2])
			}
		default:
			continue
		}
	}
}

//export phget
func phget(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var hashmap, key, value *C.char
	if C.PyArg_ParseTuple_Hashmap_Get_String(args, &hashmap, &key) == 0 {
		return C.PyLong_FromLong(0)
	}

	hashmapStr := C.GoString(hashmap)
	keyStr := C.GoString(key)
	_ = C.GoString(value)
	result := client.HGet(hashmapStr, keyStr)
	if result.Err() != nil {
		return C.PyLong_FromLong(0)
	}
	return C.PyLong_FromLong(0)
}

//export phset
func phset(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var hashmap, key, value *C.char
	if C.PyArg_ParseTuple_Hashmap_Set_String(args, &hashmap, &key, &value) == 0 {
		return C.PyLong_FromLong(0)
	}
	hashmapStr := C.GoString(hashmap)
	keyStr := C.GoString(key)
	valueStr := C.GoString(value)
	result := client.HSet(hashmapStr, keyStr, valueStr)
	if result.Err() != nil {
		return C.PyLong_FromLong(0)
	}
	return C.PyLong_FromLong(0)
}

func main() {}
