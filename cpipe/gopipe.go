package main

// #cgo pkg-config: python3
// #define Py_LIMITED_API
// #include <Python.h>
// int PyArg_ParseTuple_String(PyObject *, char**, char**, char**, char**);
// int PyArg_ParseTuple_Hashmap_Get_String(PyObject *, char**, char **);
// int PyArg_ParseTuple_Hashmap_Set_String(PyObject *, char**, char **, char **);
// int PyArg_ParseTuple_Connection(PyObject *, char**, long long *);
// int PyArg_ParseTuple_LL(PyObject *, long long *);
// PyObject* Py_String(char *pystring);
import (
	"C"
)
import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
)

const BUFFERSIZE = 1024 * 1024 * 4

type Client struct {
	sock     []*net.TCPConn
	buf      bytes.Buffer
	counter  int
	execChan chan []string
	chunks   map[int]bytes.Buffer
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

	var socks []*net.TCPConn
	for i := 0; i < 10; i++ {
		sock, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			log.Println(err)
			return C.PyLong_FromLong(0)
		}

		sock.SetNoDelay(true)
		socks = append(socks, sock)
	}

	cli = &Client{
		sock:     socks,
		chunks:   make(map[int]bytes.Buffer),
		execChan: make(chan []string, 10),
	}

	for i := 0; i < 10; i++ {
		go exec(i)
	}

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

	// Start
	switch cmdStr {
	case "hset":
		cli.buf.WriteString("*4\r\n$")
	case "hget":
		cli.buf.WriteString("*3\r\n$")
	}
	// Command
	cli.buf.WriteString(strconv.Itoa(len(cmdStr)))
	cli.buf.WriteString("\r\n")
	cli.buf.WriteString(cmdStr)
	cli.buf.WriteString("\r\n$")
	// Hashmap
	cli.buf.WriteString(strconv.Itoa(len(hashmapStr)))
	cli.buf.WriteString("\r\n")
	cli.buf.WriteString(hashmapStr)
	cli.buf.WriteString("\r\n$")
	// Key
	cli.buf.WriteString(strconv.Itoa(len(keyStr)))
	cli.buf.WriteString("\r\n")
	cli.buf.WriteString(keyStr)
	cli.buf.WriteString("\r\n")

	if cmdStr == "hset" {
		// Value
		cli.buf.WriteString("$")
		cli.buf.WriteString(strconv.Itoa(len(valueStr)))
		cli.buf.WriteString("\r\n")
		cli.buf.WriteString(valueStr)
		cli.buf.WriteString("\r\n")
	}

	if cli.buf.Len() >= BUFFERSIZE {
		cli.counter++
		cli.chunks[cli.counter] = cli.buf
		cli.buf.Reset()
	}
	return C.PyLong_FromLong(0)
}

//export execute
func execute(self *C.PyObject, args *C.PyObject) *C.PyObject {
	if cli.buf.Len() > 0 {
		cli.counter++
		cli.chunks[cli.counter] = cli.buf
		cli.buf.Reset()
	}

	for _, chunk := range cli.chunks {
	LAB:
		_, err := cli.sock[0].Write(chunk.Bytes())
		if err != nil {
			log.Println(err)
			goto LAB
		}
	}

	_ = readBuffer(cli.sock[0])

	cli.chunks = make(map[int]bytes.Buffer)
	return C.PyLong_FromLong(0)
}

func exec(bufNum int) {
	for {
		select {
		case ex, ok := <-cli.execChan:
			if !ok {
				break
			}

			buf := getRawCommand(ex)
			_, err := cli.sock[bufNum].Write(buf.Bytes())
			if err != nil {
				log.Println(err)
			}
			_ = readBuffer(cli.sock[bufNum])
			buf.Reset()
		default:
			continue
		}
	}
}

//export hget
func hget(self *C.PyObject, args *C.PyObject) *C.PyObject {

	var hashmap, key *C.char
	if C.PyArg_ParseTuple_Hashmap_Get_String(args, &hashmap, &key) == 0 {
		return C.PyLong_FromLong(0)
	}

	hashmapStr := C.GoString(hashmap)
	keyStr := C.GoString(key)
	var ex = []string{"hget", hashmapStr, keyStr}
	buf := getRawCommand(ex)
	_, err := cli.sock[0].Write(buf.Bytes())
	if err != nil {
		log.Println(err)
	}
	_ = readBuffer(cli.sock[0])
	buf.Reset()
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
	cli.execChan <- []string{"hset", hashmapStr, keyStr, valueStr}

	return C.PyLong_FromLong(0)
}

//export phget
func phget(self *C.PyObject, args *C.PyObject) *C.PyObject {

	var hashmap, key *C.char
	if C.PyArg_ParseTuple_Hashmap_Get_String(args, &hashmap, &key) == 0 {
		return C.PyLong_FromLong(0)
	}

	hashmapStr := C.GoString(hashmap)
	keyStr := C.GoString(key)

	cli.execChan <- []string{"hget", hashmapStr, keyStr}

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
	cli.execChan <- []string{"hset", hashmapStr, keyStr, valueStr}

	return C.PyLong_FromLong(0)
}

func readBuffer(sock *net.TCPConn) (buffer []string) {
	var reply = make([]byte, 4096)
	for {
		_, err := sock.Read(reply)
		if err != nil {
			log.Println("Write to server failed:", err.Error())
		}
		if err != io.EOF {
			break
		}

		buffer = append(buffer, string(reply))
	}
	return
}

func getRawCommand(ex []string) (buf bytes.Buffer) {
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(len(ex)))
	buf.WriteString("\r\n")

	for _, cmd := range ex {
		buf.WriteString("$")
		buf.WriteString(strconv.Itoa(len(cmd)))
		buf.WriteString("\r\n")
		buf.WriteString(cmd)
		buf.WriteString("\r\n")
	}

	return
}

func main() {}
