#define Py_LIMITED_API
#include <Python.h>

PyObject * execute(PyObject *, PyObject *);

int PyArg_ParseTuple_Object(PyObject * args, PyObject * result) { 
    PyObject *input;
    PyObject * strObj;
    char *cmd, *hashmap_name, *key, *value;
    int numLines;
    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &input)) {
        printf("Parse tuple error\n");    
        return 1;
    }
    numLines = PyList_Size(input);
    if (numLines < 0){
        printf("Num lines %d\n", numLines);        
        return 1;
    }
    for (int i=0; i<numLines; i++){
	    strObj = PyList_GetItem(input, i);
        if (!PyArg_ParseTuple(strObj, "ssss", &cmd, &hashmap_name, &key, &value)) {
            printf("Parse tuple error\n");    
            return 1;
        }
        printf("%s %s %s %s\n", cmd, hashmap_name, key, value);
    }
    return 0;
}

static PyMethodDef CpipeMethods[] = {  
    {"execute", execute, METH_VARARGS, ""},
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