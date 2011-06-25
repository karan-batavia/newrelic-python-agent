#ifndef PY_WRAPPER_FUNCTION_TRACE_H
#define PY_WRAPPER_FUNCTION_TRACE_H

/* ------------------------------------------------------------------------- */

/* (C) Copyright 2010-2011 New Relic Inc. All rights reserved. */

/* ------------------------------------------------------------------------- */

#include "py_transaction.h"

/* ------------------------------------------------------------------------- */

typedef struct {
    PyObject_HEAD
    NRTransactionObject *parent_transaction;
    nr_transaction_node *transaction_trace;
    nr_node_header* saved_trace_node;
    int interesting;
} NRFunctionTraceObject;

extern PyTypeObject NRFunctionTrace_Type;

typedef struct {
    PyObject_HEAD
    PyObject *dict;
    PyObject *next_object;
    PyObject *last_object;
    PyObject *name;
    PyObject *scope;
    int interesting;
} NRFunctionTraceWrapperObject;

extern PyTypeObject NRFunctionTraceWrapper_Type;

typedef struct {
    PyObject_HEAD
    PyObject *name;
    PyObject *scope;
    int interesting;
} NRFunctionTraceDecoratorObject;

extern PyTypeObject NRFunctionTraceDecorator_Type;

/* ------------------------------------------------------------------------- */

#endif

/*
 * vim: et cino=>2,e0,n0,f0,{2,}0,^0,\:2,=2,p2,t2,c1,+2,(2,u2,)20,*30,g2,h2 ts=8
 */
