#define Py_LIMITED_API
#include <Python.h>

PyObject * Connect(PyObject *, PyObject *);
PyObject * add_command(PyObject *, PyObject*);
PyObject * execute(PyObject *, PyObject *);
PyObject * hget(PyObject *, PyObject *);
PyObject * hset(PyObject *, PyObject *);

int PyArg_ParseTuple_Connection(PyObject * args, char**host, long long *port){
    return PyArg_ParseTuple(args, "sL", host, port);
}

int PyArg_ParseTuple_String(PyObject * args, char**cmd, char**hashmap_name, char **key, char **value) {
    Py_ssize_t TupleSize = PyTuple_Size(args);
    switch(TupleSize){
    case 4:
        return PyArg_ParseTuple(args, "ssss", cmd, hashmap_name, key, value);
    case 3:
        return PyArg_ParseTuple(args, "sss", cmd, hashmap_name, key);
    }
}

int PyArg_ParseTuple_Hashmap_Get_String(PyObject * args, char**hashmap_name, char **key) {
    return PyArg_ParseTuple(args, "ss", hashmap_name, key);
}

int PyArg_ParseTuple_Hashmap_Set_String(PyObject * args, char**hashmap_name, char **key, char **value) {
    return PyArg_ParseTuple(args, "sss", hashmap_name, key, value);
}

PyObject* Py_String(char **pystring){
    return Py_BuildValue("s", pystring);
}

int PyArg_ParseTuple_LL(PyObject * args, long long * count) {  
    return PyArg_ParseTuple(args, "L", count);
}

static PyMethodDef CpipeMethods[] = {
    {"Connect", Connect, METH_VARARGS, ""},
    {"add_command", add_command, METH_VARARGS, ""},
    {"execute", execute, METH_VARARGS, ""},
    {"hget", hget, METH_VARARGS, ""},
    {"hset", hset, METH_VARARGS, ""},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef cpipemodule = {
   PyModuleDef_HEAD_INIT, "cpipe", NULL, -1, CpipeMethods
};

PyMODINIT_FUNC
PyInit_cpipe(void)
{
    return PyModule_Create(&cpipemodule);
}