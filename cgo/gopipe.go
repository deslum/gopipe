package main

// #cgo pkg-config: python3
// #define Py_LIMITED_API
// #include <Python.h>
// int PyArg_ParseTuple_Object(PyObject *, PyObject *);
import "C"
import "log"

//export sum
func sum(self *C.PyObject, args *C.PyObject) {
	var a C.PyObject
	if C.PyArg_ParseTuple_Object(args, &a) == 0 {
		log.Println("This")
	}
	log.Println(a)
}

func main() {}
