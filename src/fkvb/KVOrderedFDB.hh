/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef KVORDEREDFDB_HH
#define KVORDEREDFDB_HH

#include "kv-ordered.hh"
#include "defs.hh"
#include "fkvb_test_conf.hh"
#include <string.h>


#if FDB_API == 620
#define FDB_API_VERSION 620  //to be defined *before* including fdb_c.h
#include "fdb/include/620/fdb_c.h"
#else
#error "FDB_API NOT DEFINED"
#endif

#include "fdb_helper.hh"


template<typename IO>
class KVOrderedFDB : public KVOrdered<IO> {
private:
    pthread_t _netThread;
    FDBDatabase *db;
    fkvb_test_conf *conf;

public:
    KVOrderedFDB(fkvb_test_conf *conf);

    int get(const char key[], size_t key_size, char *val_buff, size_t val_buff_size, size_t &val_size_read,
            size_t &val_size);

    int shutdown();

    int init();

    int put(const char key[], size_t key_size, const char *val, size_t val_size);

    int del(const char key[], size_t key_size);

    int put_bulk(size_t numkv, char *k_ptr, size_t *k_sizes, char *v_ptrs, size_t *v_sizes);

    int generic(int num_op, bool *rw, char *keys, size_t *key_sizes, char **put_values,
                size_t *put_value_sizes, char *get_buffer, size_t get_buffer_size, size_t *read_values,
                std::vector<char *> &read_values_ptr);

    unsigned long get_size() const;

    unsigned long get_raw_capacity() const;

    void thread_local_entry();

    void thread_local_exit();

    int get_range(const char start_key[], size_t start_key_size, const char end_key[], size_t end_key_size,
                  char *kv_buff, size_t kv_buff_size, size_t &kv_size_read, std::vector<char *> &kv_ptrs);

    void print_stats() {

    }


};


#endif //KVORDEREDFDB_HH
