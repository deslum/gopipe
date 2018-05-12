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
)

const (
	PIPELINE = "*4\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n"
)

type Client struct {
	sock *net.TCPConn
	buf  string
}

var cli *Client

//export Connect
func Connect(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var ip *C.char
	var port C.longlong
	if C.PyArg_ParseTuple_Connection(args, &ip, &port) == 0 {
		log.Println(0)
		return C.PyLong_FromLong(0)
	}
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", C.GoString(ip), int(port)))
	if err != nil {
		log.Println(err)
		return C.PyLong_FromLong(0)
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Println(err)
		return C.PyLong_FromLong(0)
	}
	cli = &Client{sock: sock}
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
	cli.buf += fmt.Sprintf(PIPELINE,
		len(cmdStr), cmdStr,
		len(hashmapStr), hashmapStr,
		len(keyStr), keyStr,
		len(valueStr), valueStr)
	return C.PyLong_FromLong(0)
}

//export execute
func execute(self *C.PyObject, args *C.PyObject) *C.PyObject {
	_, err := cli.sock.Write([]byte(cli.buf))
	if err != nil {
		log.Println(err)
	}
	cli.buf = ""
	return C.PyLong_FromLong(0)
}

func main() {}
