package main

// #cgo pkg-config: python3
// #define Py_LIMITED_API
// #include <Python.h>
// int PyArg_ParseTuple_Object(PyObject *, PyObject *);
import "C"

//export execute
func execute(self *C.PyObject, args *C.PyObject) *C.PyObject {
	var a C.PyObject
	C.PyArg_ParseTuple_Object(args, &a)
	return C.PyLong_FromLong(0)
}

func main() {}
