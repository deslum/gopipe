#define Py_LIMITED_API
#include <Python.h>

PyObject * sum(PyObject *, PyObject *);

int PyArg_ParseTuple_Object(PyObject * args, PyObject * a) { 
    PyObject *input;
    PyObject * strObj;
    char *cmd, *hashmap_name;
    char *key;
    char *value;
    int numLines;
    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &input)) {
        printf("Parse tuple error\n");    
        return 0;
    }
    numLines = PyList_Size(input);
    if (numLines < 0){
        printf("Num lines %d\n", numLines);        
        return 0;
    }
    for (int i=0; i<numLines; i++){
	    strObj = PyList_GetItem(input, i);
        if (!PyArg_ParseTuple(strObj, "ssss", &cmd, &hashmap_name, &key, &value)) {
            printf("Parse tuple error\n");    
            return 0;
        }
        printf("%s %s %s %s\n", cmd, hashmap_name, key, value);
    }
    return PyList_Check(&a);
}

static PyMethodDef CpipeMethods[] = {  
    {"sum", sum, METH_VARARGS, "Add two numbers."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef cpipemodule = {  
   PyModuleDef_HEAD_INIT, "foo", NULL, -1, CpipeMethods
};

PyMODINIT_FUNC  
PyInit_cpipe(void)  
{
    return PyModule_Create(&cpipemodule);
}