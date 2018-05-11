package main

// #cgo pkg-config: python3
// #define Py_LIMITED_API
// #include <Python.h>
// int PyArg_ParseTuple_String(PyObject *, char**, char**, char**, char**);
// int PyArg_ParseTuple_Connection(PyObject *, char**, long long *);
// int PyArg_ParseTuple_LL(PyObject *, long long *);
// PyObject* Py_String(char *pystring);
import (
	"C"
)
import (
	"fmt"
	"log"
	"net"
	"sync"
)

const (
	PIPELINE = "*4\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n"
)

var pipeline []string

//export Connect
func Connect(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var ip *C.char
	var port C.longlong
	if C.PyArg_ParseTuple_Connection(args, &ip, &port) == 0 {
		return C.PyLong_FromLong(0)
	}
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", C.GoString(ip), int(port)))
	if err != nil {
		return C.PyLong_FromLong(0)
	}

	_, err = net.DialTCP("tcp", nil, addr)
	if err != nil {
		return C.PyLong_FromLong(0)
	}
	log.Println(fmt.Sprintf("%s:%d", C.GoString(ip), int(port)))
	return C.PyLong_FromLong(0)
}

//export add_command
func add_command(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var cmd, hashmap, key, value *C.char
	if C.PyArg_ParseTuple_String(args, &cmd, &hashmap, &key, &value) == 0 {
		return C.PyLong_FromLong(0)
	}
	cmdStr := C.GoString(cmd)
	hashmapStr := C.GoString(hashmap)
	keyStr := C.GoString(key)
	valueStr := C.GoString(value)
	str := fmt.Sprintf(PIPELINE,
		len(cmdStr), cmdStr,
		len(hashmapStr), hashmapStr,
		len(keyStr), keyStr,
		len(valueStr), valueStr)
	pipeline = append(pipeline, str)
	return C.PyLong_FromLong(0)
}

//export execute
func execute(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var returnStr string
	var count C.longlong
	wg := new(sync.WaitGroup)
	ch := make(chan string)
	if C.PyArg_ParseTuple_LL(args, &count) == 0 {
		return C.PyLong_FromLong(0)
	}
	for i := 0; i < int(count); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				str, ok := <-ch
				if !ok {
					break
				}
				returnStr += str
			}
		}()
	}
	for _, pipe := range pipeline {
		ch <- pipe
	}
	close(ch)
	wg.Wait()
	return C.Py_String(C.CString(returnStr))
}

func main() {}
