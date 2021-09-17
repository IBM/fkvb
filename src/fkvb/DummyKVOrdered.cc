/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#include "DummyKVOrdered.hh"

u32 keys;

template<typename IO>
DummyKVOrdered<IO>::DummyKVOrdered(fkvb_test_conf *conf) {
    keys = conf->num_keys;
}

template<typename IO>
int
DummyKVOrdered<IO>::get(const char key[], size_t key_size, char *val_buff, size_t val_buff_size, size_t &val_size_read,
                        size_t &val_size) {
    return 0;
}

template<typename IO>
int DummyKVOrdered<IO>::generic(int num_op, bool *rw, char *keys, size_t *key_sizes, char **put_values, size_t *put_value_sizes,
        char *get_buffer,
        size_t get_buffer_size, size_t *read_values, std::vector<char *> &read_values_ptr) {
    int i;
    for (i = 0; i < num_op; i++) {

    }
    return 0;
};

template<typename IO>
int DummyKVOrdered<IO>::put_bulk(size_t numkv, char *k_ptr, size_t *k_sizes, char *v_ptrs, size_t *v_sizes) {
    size_t i;
    int rc;
    char *k = k_ptr, *v = v_ptrs;
    for (i = 0; i < numkv; i++) {
        rc = put(k, k_sizes[i], v, v_sizes[i]);
        k += k_sizes[i];
        v += v_sizes[i];
        if (rc)return rc;
    }
    return 0;
}


template<typename IO>
int DummyKVOrdered<IO>::shutdown() {
    return 0;
}

template<typename IO>
int DummyKVOrdered<IO>::init() {
    return 0;
}

template<typename IO>
int DummyKVOrdered<IO>::put(const char key[], size_t key_size, const char *val, size_t val_size) {
    return 0;
}

template<typename IO>
int DummyKVOrdered<IO>::del(const char key[], size_t key_size) {
    return 0;
}

template<typename IO>
unsigned long DummyKVOrdered<IO>::get_size() const {
    return keys;
}

template<typename IO>
unsigned long DummyKVOrdered<IO>::get_raw_capacity() const {
    return 0UL;
}

template<typename IO>
void DummyKVOrdered<IO>::thread_local_entry() {

}

template<typename IO>
void DummyKVOrdered<IO>::thread_local_exit() {

}

template<typename IO>
int
DummyKVOrdered<IO>::get_range(const char start_key[], size_t start_key_size, const char end_key[], size_t end_key_size,
                              char *kv_buff, size_t kv_buff_size, size_t &kv_size_read, std::vector<char *> &kv_ptrs) {
    return 0;
}

template
class DummyKVOrdered<int>;
