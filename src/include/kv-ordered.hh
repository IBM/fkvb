/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *           Nikolas Ioannou (nio@zurich.ibm.com)
 *
 */
#ifndef    _KV_ORDERED_H_
#define    _KV_ORDERED_H_

#include "kv.hh"

#include <vector>
#include <stdio.h>

// TODO: add iterator
// TODO: add also the KVMbuff Iface to support 0copy
template<typename IO>
class KVOrdered : public virtual KV {
    // Inherits get, put, del from KV
public:
    virtual int get_range(const char start_key[], size_t start_key_size,
                          const char end_key[], size_t end_key_size,
                          char *kv_buff,         // user provided buffer
                          size_t kv_buff_size,  // size of user provided buffer
                          size_t &kv_size_read, // size of all kv pairs read so far (<= kv_buff_size)
                          std::vector<char *> &kv_ptrs // ptrs to kv pairs returned in kv_buff, in order
    ) = 0;

    virtual int put_bulk(size_t numkv, char *k_ptr, size_t *k_sizes, char *v_ptrs, size_t *v_sizes) = 0;

    virtual void print_stats() = 0;

    virtual int generic(int num_op, bool *rw, char *keys, size_t *key_sizes, char **put_values, size_t *put_value_sizes, char *get_buffer,
                        size_t get_buffer_size, size_t *read_values, std::vector<char *> &read_values_ptr) = 0;
};

#endif //_KV_ORDERED_H_
