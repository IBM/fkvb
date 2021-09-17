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

#include "kv-conf.hh"

#include <string>
#include <cstdio>
#include <unistd.h>
#include <stdexcept>      // std::invalid_argument

#include "debug.hh"


KV_conf::KV_conf(const std::string &fname) :
        type_m(KV_LAST),
        thread_nr_m(1),
        fname_m(fname) {}

bool
KV_conf::valid_kv_type(const kv_type type) {
    switch (type) {
        case KV_FDB:
        case KV_DUMMY:
            return true;
        case KV_LAST:
        default:
            return false;
    }
}

int
KV_conf::parse_args(ParseArgs &args) {

    const int argc = args.argc_;
    const char *const *const argv = args.argv_;

    for (int i = 1; i < argc; ++i) {
        std::string arg = std::string(argv[i]);
        try {
            // all arguments take at least one value, except help
            std::string val = i < argc - 1 ? std::string(argv[i + 1]) : "";
            DBG("arg given %s (val=%s).\n", arg.c_str(), val.c_str());
            if ("-t" == arg || "--threads" == arg) {
                thread_nr_m = std::stoi(val);
                args.used_arg_and_val(i);
                ++i;
            } else if ("-u" == arg || "--udepot" == arg) {
                int type = std::stoi(val);
                args.used_arg_and_val(i);
                ++i;
                if (type < KV_LAST)
                    type_m = static_cast<enum kv_type>(type);
            } else if ("-h" == arg || "--help" == arg) {
                args.used_arg(i);
                help_m = true;
                return 0;
            } else if ("-f" == arg || "--file" == arg) {
                fname_m = val;
                args.used_arg_and_val(i);
                ++i;
            }
        } catch (std::invalid_argument &) {  //might be  std::invalid_argument & on some machines
            ERR("invalid argument for %s\n", arg.c_str());
            return EINVAL;
        }
    }

    if (!valid_kv_type(type_m)) {
        ERR("Invalid kv type given.\n");
        return EINVAL;
    } else {
        printf("KV type %s\n", type_to_string(type_m).c_str());
    }
    return 0;
}

std::string
KV_conf::type_to_string(const kv_type type) {
    switch (type) {
        case KV_FDB:
            return std::string("FDB");
        case KV_DUMMY:
            return std::string("DUMMY");
        case KV_LAST:
            return std::string("__LAST__");
    }
    return std::string("Invalid type");
}

void
KV_conf::print_usage(const char name[]) {
    printf("Usage: %s\n", name);
    printf("Compulsory arguments:\n");
    printf("-f, --file path: path to file that contains the KV data for this machine.\n\n");
    printf("Optional arguments:\n");
    printf("-t, --threads nr: Number of KV client threads to be spawned on this machine\n");
    printf("-u, --udepot TYPE: type of KV implementation to be used:\n");
    for (u32 i = 0; i < KV_LAST; i++) {
        auto ty = static_cast<kv_type>(i);
        auto disabled = valid_kv_type(ty) ? "" : " (disabled at compile-time)";
        printf(" %2u: %s%s\n", i, type_to_string(ty).c_str(), disabled);
    }
}

void KV_conf::validate_and_sanitize_parameters() {
    if (fname_m == "") {
        printf("Parameter -f not provided\n");
        abort();
    }
}

