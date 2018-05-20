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
	"bytes"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"
)

type Client struct {
	sock   *net.TCPConn
	buf    bytes.Buffer
	chunks []bytes.Buffer
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
	var buffer bytes.Buffer
	var cmd, hashmap, key, value *C.char
	if C.PyArg_ParseTuple_String(args, &cmd, &hashmap, &key, &value) == 0 {
		return C.PyLong_FromLong(0)
	}

	cmdStr := C.GoString(cmd)
	hashmapStr := C.GoString(hashmap)
	keyStr := C.GoString(key)
	valueStr := C.GoString(value)

	// Start
	buffer.WriteString("*4\r\n$")
	// Command
	buffer.WriteString(strconv.Itoa(len(cmdStr)))
	buffer.WriteString("\r\n")
	buffer.WriteString(cmdStr)
	buffer.WriteString("\r\n$")
	// Hashmap
	buffer.WriteString(strconv.Itoa(len(hashmapStr)))
	buffer.WriteString("\r\n")
	buffer.WriteString(hashmapStr)
	buffer.WriteString("\r\n$")
	// Key
	buffer.WriteString(strconv.Itoa(len(keyStr)))
	buffer.WriteString("\r\n")
	buffer.WriteString(keyStr)
	buffer.WriteString("\r\n$")
	// Value
	buffer.WriteString(strconv.Itoa(len(valueStr)))
	buffer.WriteString("\r\n")
	buffer.WriteString(valueStr)
	buffer.WriteString("\r\n")

	cli.buf.Write(buffer.Bytes())
	if cli.buf.Len() > 6000 {
		cli.chunks = append(cli.chunks, cli.buf)
		cli.buf.Reset()
	}
	return C.PyLong_FromLong(0)
}

//export execute
func execute(self *C.PyObject, args *C.PyObject) *C.PyObject {
	if cli.buf.Len() > 0 {
		cli.chunks = append(cli.chunks, cli.buf)
		cli.buf.Reset()
	}

	for _, chunk := range cli.chunks {
		_, err := cli.sock.Write(chunk.Bytes())
		if err != nil {
			log.Println(err)
			log.Fatalln(chunk.String)
		}
		time.Sleep(500 * time.Microsecond)
	}
	cli.chunks = cli.chunks[:0]
	return C.PyLong_FromLong(0)
}

func main() {}
