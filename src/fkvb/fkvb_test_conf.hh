/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */
#ifndef FKVB_TEST_CONF_HH
#define FKVB_TEST_CONF_HH

#include "kv-conf.hh"
#include "types.hh"
#include "debug.hh"
#include <inttypes.h>
#include "kv-ordered.hh"
#include <string.h>
#include "defs.hh"

using namespace udepot;

#define UNIFORM "uniform" //
#define ZIPFIAN "zipf"//
#define CONSTANT "const"//


struct fkvb_test_conf : public KV_conf {

#define DEFAULT_KEY_SIZE 16
#define DEFAULT_VALUE_SIZE_GEN "const8"
#define DEFAULT_NUM_KEYS 10000
#define DEFAULT_SCAN_LENGTH "const10"
#define DEFAULT_READ_PERC 100
#define DEFAULT_UPDATE_PERC 0
#define DEFAULT_INSERT_PERC 0
#define DEFAULT_SCAN_PERC 0
#define DEFAULT_GENERIC_PERC 0
#define DEFAULT_GENERIC_OPS 10
#define DEFAULT_GENERIC_RP 0
#define DEFAULT_RMW_PERC 0
#define DEFAULT_DAP UNIFORM
#define DEFAULT_DURATION "sec60"
#define DEFAULT_ADDITIONAL_ARGS ""
#define DEFAULT_KEY_TYPE "fkvb"
#define DEFAULT_IO "direct"


    fkvb_test_conf()
            : KV_conf(""),
              read_perc(DEFAULT_READ_PERC),
              update_perc(DEFAULT_UPDATE_PERC),
              insert_perc(DEFAULT_INSERT_PERC),
              rmw_perc(DEFAULT_RMW_PERC),
              scan_perc(DEFAULT_SCAN_PERC),
              generic_perc(DEFAULT_GENERIC_PERC),
              generic_rp(DEFAULT_GENERIC_RP),
              generic_ops(DEFAULT_GENERIC_OPS),
              num_keys(DEFAULT_NUM_KEYS),
              frequency(0),
              num_population_threads(1),
              key_size(DEFAULT_KEY_SIZE),
              key_gen(UNIFORM),
              scan_len_gen(DEFAULT_SCAN_LENGTH),
              value_size_gen(DEFAULT_VALUE_SIZE_GEN),
              xput_file(""),
              duration(DEFAULT_DURATION),
              io(DEFAULT_IO),
              seed(-1),
              load_barrier_file(""),
              load_barrier(load_barrier_file == "" ? false : true),
              instance_id(0), num_instances(1), key_type(DEFAULT_KEY_TYPE), additional_args(DEFAULT_ADDITIONAL_ARGS),
              config_file(""),
	      sleep_time_us(0),
	      grv_cache_ms(0){}

    ~fkvb_test_conf() {};

    u8 read_perc, update_perc, insert_perc, rmw_perc, scan_perc, generic_perc, generic_rp;
    u32 generic_ops, num_keys, frequency, num_population_threads, key_size;
    std::string key_gen, scan_len_gen, value_size_gen, xput_file, duration, io;
    long seed;
    std::string load_barrier_file;
    bool load_barrier;
    u32 instance_id, num_instances;
    std::string key_type, additional_args, config_file;
    u32 sleep_time_us;	   
    //FDB specific
    u32 grv_cache_ms=0;

    int parse_args(ParseArgs &args) override final;

    //void print_usage(const char []) override final;

    void validate_and_sanitize_parameters() override final;

    void print_usage(const char name[]);

};

#endif
