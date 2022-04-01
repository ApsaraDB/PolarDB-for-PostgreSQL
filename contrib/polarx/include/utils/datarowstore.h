/*-------------------------------------------------------------------------
 *
 * datarowstore.h
 *	  Generalized routines for temporary datarowstore storage.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/polarx/include/utils/datarowstore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATAROWSTORE_H
#define DATAROWSTORE_H

#include "executor/tuptable.h"


/*
 * Represents a DataRow message received from a remote node.
 * Contains originating node number and message body in DataRow format without
 * message code and length. Length and node number are separate fields.
 * This is a variable length structure.
 */
typedef struct RemoteDataRowData
{
    Oid         msgnode;                /* node number of the data row message */
    int         msglen;                    /* length of the data row message */
    char        msg[0];                    /* last data row message */
}     RemoteDataRowData;
typedef RemoteDataRowData *RemoteDataRow;
/*
 * Datarowstorestate is an opaque type whose details are not known outside
 * datarowstore.c.
 */
typedef struct Datarowstorestate Datarowstorestate;

extern void datarowstore_putdatarow(Datarowstorestate *state,
                                    RemoteDataRow datarow);

extern bool datarowstore_getdatarow(Datarowstorestate *state, bool forward,
                                    bool copy, RemoteDataRow *datarow);

extern void datarowstore_end(Datarowstorestate *state);

extern Datarowstorestate *datarowstore_begin_datarow(bool interXact, int maxKBytes);
#endif							/* DATAROWSTORE_H */
