/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#include "fkvb_test_conf.hh"


void fkvb_test_conf::validate_and_sanitize_parameters() {
    const unsigned int total = read_perc + scan_perc + update_perc + insert_perc + rmw_perc + generic_perc;
    if (100 != total) {
        FATAL("Sum of ops is not 100 (but %u). Read %u scan %u update %u insert %u rmw %u generic %u", total, read_perc,
              scan_perc, update_perc, insert_perc, rmw_perc, generic_perc);
        exit(1);
    }
    if (generic_perc && value_size_gen.find("const") == std::string::npos) {
        FATAL("Generic ops are only supported with fixed values.")
        exit(1);
    }
    if (generic_perc && 0 == generic_ops) {
        //FATAL("Generic ops are defined but number is 0.")
        //exit(1);
        fprintf(stdout,"Generic ops is 0");
    }

    if (!frequency) {
        FATAL("The frequency has not been set (--freq).")
    }
#define MIN_FREQ_       1000000000ULL // assume min 1GHz
    if (frequency < MIN_FREQ_) {
        FATAL("The frequency has been set to less than 1GHz.")
    }
    if (xput_file == "") {
        FATAL("The xput measurement file has not been set (--xput).")
    }
    if (duration.find("sec") == std::string::npos && duration.find("ops") == std::string::npos) {
        FATAL("Could not recognize operation type %s. Can be secX or opsX", duration.c_str());
    }

    if (key_type != "fkvb" && key_type != "random" && key_type !="sharded_fkvb" && key_type !="sharded_random") {
        FATAL("key_type must be (sharded_)fkvb or (sharded_)random or ");
    }
    if (config_file == "" && type_m == KV_FDB) {
        FATAL("Config file not specified");
    }


}

void fkvb_test_conf::print_usage(const char name[]) {
    KV_conf::print_usage(name);
    printf("\n");
    printf("fkvb-test specific parameters.\n");
    printf("Compulsory arguments:\n");
    printf("--freq: Number of tics in one second. E.g., if the CPU is @2.2Ghz, freq should be 2200000000. Remember to disable frequency scaling.\n");
    printf("--xput: File used to dump xput measurements.\n>>> (Don't put it in the nsf-backed /home if running as root!).\n");
    printf("Optional arguments (if not set, the corresponding default value is assigned):\n");
    printf("--num_keys: Number of keys. Default = %u).\n", DEFAULT_NUM_KEYS);
    printf("--dap: Data access pattern. It can be \"uniform\" or \"zipfian\". Default = %s\n", DEFAULT_DAP);
    printf("--ks: Key size. Key with index k is in the form \"User: 0k\", with as many trailing 0s to fill the desired size."
           " Default = %u (the size of the key should be consistent with the maximum number of digits a key index can take)\n",
           DEFAULT_KEY_SIZE);
    printf("--value_size: Distribution of the size of the values in bytes. It can be constx, or uniformx_y. Default = %s\n",
           DEFAULT_VALUE_SIZE_GEN);
    printf("--read_perc: Percentage of point read operations. Default = %u\n", DEFAULT_READ_PERC);
    printf("--update_perc: Percentage of point update operations. Default = %u\n", DEFAULT_UPDATE_PERC);
    printf("--rmw_perc: Percentage of point read-modify-write operations. Default = %u\n", DEFAULT_RMW_PERC);
    printf("--scan_perc: Percentage of scan operations. Default = %u\n", DEFAULT_SCAN_PERC);
    printf("--scan_len: Distribution of the length of scan operations. It can be constx or uniformx_y. Default = %s\n",
           DEFAULT_SCAN_LENGTH);
    printf("--generic_perc: Percentage of generic multi-operation transactions. Default = %u\nNOTE: generic operations are only supported with *fixed* value sizes\n",
           DEFAULT_GENERIC_PERC);
    printf("--generic_ops: Operations per generic transactions. Default = %u\n", DEFAULT_GENERIC_OPS);
    printf("--generic_rp: Percentage of read operations within a generic transaction (the remainder are updates). Default = %u\n",
           DEFAULT_GENERIC_RP);

    printf("--t_population: Number of threads used to load data in the kv store (default 1).\n");
    printf("--seed: Seed used to initialize random generators (default: -1, which is reserved to set the seed of each generator to the  wallclock time upon creation)\n");
    printf("--dur: Duration of the experiment. It can be either opsX (X ops per thread) or secX (X seconds). Default = sec60");
    printf("--load_barrier: barrier file to block the  process after the population until a SIGUSR2 is received. Default = \"\", i.e., do NOT use barrier.\n>>> (Don't use a file that is in the nsf-backed /home if running as root!))");

    printf("--id: id of the client. Needed to support parallel load (default 1).\n");
    printf("--num_clients: number of client processes. Needed to support parallel load (default 1)\n");
    printf("--key-type: format of the key used. Possible values are fkvb and random (default %s). Keys have a prefix and a numerical suffix id"
           "to make them unique. fkvb keys have the suffix of the form User: 00...00N. random keys have a random prefix followed by the id N",
           DEFAULT_KEY_TYPE);
    printf("\n");
}


int fkvb_test_conf::parse_args(ParseArgs &args) {
    int rc = KV_conf::parse_args(args);

    if (rc != 0) {
        ERROR("KV parse is giving error");
        return rc;
    }

    const int argc = args.argc_;
    const char *const *const argv = args.argv_;
    //TODO
    for (int i = 1; i < argc; ++i) {
        std::string arg = std::string(argv[i]);
        // all arguments take at least one value, except help
        std::string val = i < argc - 1 ? std::string(argv[i + 1]) : "";
        fprintf(stdout, "arg given %s.\n", arg.c_str());
        if (args.used_[i]) {
            continue;  //Already parsed by previous call
        }
        if ("--read_perc" == arg) {
            read_perc = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Read perc is %"
                                 P8, read_perc);
            ++i;
            continue;
        }
        if ("--generic_perc" == arg) {
            generic_perc = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("generic perc is %"
                                 P8, generic_perc);
            ++i;
            continue;
        } else if ("--t_population" == arg) {
            num_population_threads = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Num population threads is %u", num_population_threads);
            ++i;
            continue;
        } else if ("--scan_perc" == arg) {
            scan_perc = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Scan perc is %"
                                 P8, scan_perc);
            ++i;
            continue;
        } else if ("--update_perc" == arg) {
            update_perc = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Update perc is %"
                                 P8,
                         update_perc);
            ++i;
            continue;
        } else if ("--insert_perc" == arg) {
            insert_perc = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Insert perc is %"
                                 PRIu8, insert_perc);
            ++i;
            continue;
        } else if ("--rmw_perc" == arg) {
            rmw_perc = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("RMW perc is %"
                                 PRIu8, rmw_perc);
            ++i;
            continue;
        } else if ("--generic_rp" == arg) {
            generic_rp = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Generic read perc is %"
                                 PRIu8, generic_rp);
            ++i;
            continue;
        } else if ("--generic_ops" == arg) {
            generic_ops = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Generic read ops is %u", generic_ops);
            ++i;
            continue;
        } else if ("--dap" == arg) {
            key_gen = val;
            args.used_arg_and_val(i);
            PRINT_FORMAT("DAP is %s", key_gen.c_str());
            ++i;
        } else if ("--scan_len" == arg) {
            scan_len_gen = val;
            args.used_arg_and_val(i);
            PRINT_FORMAT("SCAN_L is %s", scan_len_gen.c_str());
            ++i;
            continue;
        } else if ("--num_keys" == arg) {
            num_keys = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Num keys is %u", num_keys);
            ++i;
            continue;
        } else if ("--seed" == arg) {
            seed = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Seed is %ld", seed);
            ++i;
            continue;
        } else if ("--value_size" == arg) {
            value_size_gen = val;
            args.used_arg_and_val(i);
            PRINT_FORMAT("Value size generator is %s", value_size_gen.c_str());
            ++i;
        } else if ("--xput" == arg) {
            xput_file = val;
            args.used_arg_and_val(i);
            PRINT_FORMAT("Xput file is %s", xput_file.c_str());
            ++i;
            continue;
        } else if ("--freq" == arg) {
            frequency = stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Frequency is %u", frequency);
            ++i;
        } else if ("--key_size" == arg) {
            key_size = (u32) stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Key size is %u", key_size);
            ++i;
            continue;
        } else if ("--dur" == arg) {
            duration = (val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Duration is %s", duration.c_str());
            ++i;
            continue;
        } else if ("--id" == arg) {
            instance_id = (u32) stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Instance id %u", instance_id);
            ++i;
            continue;
        } else if ("--num_clients" == arg) {
            num_instances = (u32) stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Num clients: %u", num_instances);
            ++i;
            continue;
        } else if ("--load_barrier" == arg) {
            load_barrier_file = val;
            args.used_arg_and_val(i);
            if (load_barrier_file != "") {
                load_barrier = true;
            }
            PRINT_FORMAT("Load barrier file = %s", load_barrier_file.c_str());
            ++i;
            continue;
        } else if ("--key_type" == arg) {
            key_type = std::string(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("key type is %s", key_type.c_str());
            ++i;
            continue;
        } else if ("--io" == arg) {
            io = std::string(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("io %s", io.c_str());
            ++i;
            continue;
        } else if ("--additional_args" == arg) {
            additional_args = std::string(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Additional args are %s", val.c_str());
            ++i;
        } else if ("--config_file" == arg) {
            config_file = std::string(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("Config file is %s", val.c_str());
            ++i;
        } else if ("--grv_cache_ms" == arg) {
            grv_cache_ms = (u32) stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("grv_cache_ms  is %u", grv_cache_ms);
            ++i;
        } else if ("--sleep_time_us" == arg) {
            sleep_time_us = (u32) stoul(val);
            args.used_arg_and_val(i);
            PRINT_FORMAT("sleep_time_us  is %u", sleep_time_us);
            ++i;
        } else {
            PRINT_FORMAT(">>>> Unknown flag %s", arg.c_str());
            return 1;
        }
    }
    return 0;
}
