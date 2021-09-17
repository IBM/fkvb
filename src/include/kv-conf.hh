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
#ifndef KV_CONF_H_
#define KV_CONF_H_

#include <string>
#include <vector>
#include <cstdint>
#include <iostream>

struct ParseArgs;

struct KV_conf {

    KV_conf(const std::string & fname);

    virtual ~KV_conf() {};
    enum kv_type {
        KV_FDB = 13,
        KV_DUMMY = 14,
        KV_LAST,
    };
    bool help_m;
    enum kv_type type_m;
    uint32_t thread_nr_m;

    std::string fname_m;


    static std::string type_to_string(kv_type);

    static bool valid_kv_type(kv_type);

    virtual int parse_args(ParseArgs &args);

    virtual void print_usage(const char []);

    virtual void validate_and_sanitize_parameters();
};

struct ParseArgs {
    const int argc_;
    const char *const *const argv_;
    std::vector<bool> used_; // bitset of used arguments, to track invalid arguments at the end

    ParseArgs(const int argc, const char *const *const argv)
            : argc_(argc), argv_(argv), used_(argc, false) {}

    void used_arg(int i) { used_[i] = true; }

    void used_arg_and_val(int i) {
        used_arg(i);
        used_arg(i + 1);
    }

    bool has_unused_args(void) {
        for (int i = 1; i < argc_; i++) { // ignore argv[0]
            if (!used_[i])
                return true;
        }
        return false;
    }

    void print_unused_args(std::ostream &out) {
        for (int i = 1; i < argc_; i++) { // ignore argv[0]
            if (!used_[i]) {
                if (argv_[i][0] == '-') {
                    out << " Unused (invalid?) option:" << argv_[i] << std::endl;
                } else {
                    out << " Unused argument:" << argv_[i] << std::endl;
                }
            }
        }
    }
};


#endif    
