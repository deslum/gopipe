package main

// #cgo pkg-config: python3
// #define Py_LIMITED_API
// #include <Python.h>
// int PyArg_ParseTuple_String(PyObject *, char**, char**, char**, char**);
// PyObject* Py_String(char *pystring);
import "C"
import "fmt"

const (
	PIPELINE = "*4\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n"
)

var pipeline []string

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
	for _, pipe := range pipeline {
		returnStr += pipe
	}
	return C.Py_String(C.CString(returnStr))
}

func main() {}
