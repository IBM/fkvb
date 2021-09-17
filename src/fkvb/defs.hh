/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */


#ifndef DEFS_HH
#define DEFS_HH

//#define PRODUCTION

#include "types.hh"
#include <stdlib.h>


#define  P64    PRIu64
#define  P32    PRIu32
#define  P16    PRIu16
#define  P8     PRIu8

//#define TRACE

#define UNUSED(x) (void)(x)

#ifdef PRODUCTION
#define PRINT_FORMAT(format, arg...)
#define PRINT(arg)
#define TRACE_FORMAT(format, arg...)


#else //Not production
#define PRINT_FORMAT(format, arg...)                \
    do {                        \
        fprintf(stdout, "%s:%d:  " format "\n",    \
            __FUNCTION__, __LINE__, ##arg); \
    } while (0)

#define PRINT(arg)                \
    do {                        \
        fprintf(stdout, "%s:%d:  %s\n",    \
            __FUNCTION__, __LINE__, arg); \
    } while (0)


#ifdef TRACE
#define TRACE_FORMAT(format, arg...) do {                        \
        fprintf(stdout, "%s:%d:  " format "\n",    \
            __FUNCTION__, __LINE__, ##arg); \
    } while (0)
#else
#define TRACE_FORMAT(format, arg...)
#endif


#endif //PRODUCTION


enum OPS {
    OP_READ = 1, OP_UPDATE = 2, OP_INSERT = 3, OP_SCAN = 4, OP_RMW = 5, OP_GENERIC = 6,
    OP_INIT = 7, OP_COMMIT = 8, OP_LAST = 9
};


//FATAL is always defined
#define FATAL(format, arg...)                \
    do {                        \
        fprintf(stdout, "[FATAL ERROR] %s:%d:  " format "\n",    \
            __FUNCTION__, __LINE__, ##arg); fflush(stdout); \
        fprintf(stderr, "[FATAL ERROR] %s:%d:  " format "\n",    \
            __FUNCTION__, __LINE__, ##arg); fflush(stderr);\
            exit(1); \
    } while (0);


//ERROR is always defined
/*&#define ERROR(arg)                \
    do {                        \
        fprintf(stdout, "[ERROR] %s:%d:  %s\n",    \
            __FUNCTION__, __LINE__, arg); \
        fprintf(stderr, "[ERROR] %s:%d:  %s\n",    \
            __FUNCTION__, __LINE__, arg); \
    } while (0)
*/
#define ERROR(format, arg...)                \
    do {                        \
        fprintf(stdout, "[ERROR] %s:%d:  " format "\n",    \
            __FUNCTION__, __LINE__, ##arg);fflush(stdout); \
        fprintf(stderr, "[ERROR] %s:%d:  " format "\n",    \
            __FUNCTION__, __LINE__, ##arg);fflush(stderr); \
    } while (0);
#endif //DEFS_HH

#define NOT_IMPLEMENTED FATAL("Function not implemented");
