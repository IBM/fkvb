/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef FKVB_HH
#define FKVB_HH


#include "fkvb_test_conf.hh"
#include <cstddef>
#include "kv-conf.hh"
#include "types.hh"
#include "debug.hh"
#include <inttypes.h>
#include "rnd/fkvb_rnd.hh"
#include "defs.hh"
#include <pthread.h>
#include <unistd.h>
#include "defs.hh"
#include <algorithm>
#include "StringBuilder.hh"
#include <chrono>
#include <cstring>
#include "profiling.hh"
#include "FKVB_g.hh"
#include "reservoir.hh"
#include <ticks.hh>
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>

#define BULK_SIZE 20

//#define TRACE_PERF  //Enable/disable reservoir sampling-based statistics
#define XPUT_SAMPLES 172800  //48 hrs at one second interval

#ifdef CONF_SDT
#define Y_PROBE_TICKS_START(id) state->last_init= ticks::get_ticks();

#define Y_PROBE_TICKS_END(id) do { \
    state->last_duration = ticks::get_ticks() - state->last_init;  \
    DTRACE_PROBE1(udepot, id,state->last_duration);                \
} while (0)
#else//NO CONF_SDT
#define Y_PROBE_TICKS_START(id)  state->last_init= ticks::get_ticks();
#define Y_PROBE_TICKS_END(id) state->last_duration = ticks::get_ticks() - state->last_init;
#endif//CONF_SDT


#define USE_RESERVOIR 1

template<typename IO>
class FKVB : public FKVB_g {
private:

    volatile bool population_barrier = true; //If/when true, the test starts right after population. Else, wait for SIGUSR2

    struct next_op_pattern {
        struct rand_io_pattern *rnd;
        OPS ops[100];
        u8 curr_i = 0;

        /* TO ADD when we add seed to state
        next_op_pattern(fkvb_thread_state *state) {
            rnd = new rand_io_pattern(100, 0, 0);
        }
         */

        next_op_pattern() {
            rnd = new rand_io_pattern(100, 0, get_random_seed());
        }

        next_op_pattern(long seed) {
            rnd = new rand_io_pattern(100, 0, seed);
        }

        void add_ops(const OPS type, const u8 perc) {
            u8 i = 0;
            while (i++ < perc) {
                ops[curr_i] = type;
                curr_i += 1;
            }
        }

        OPS next() {
            const int next = rnd->next();
            const OPS op = ops[next];
            return op;
        }

        ~next_op_pattern() { delete rnd; };


    };

    enum duration_type {
        OPS_N = 1, SEC_N = 2
    };

    struct xput_sample {
        u32 time;
        u32 ops;
        u64 cumul; //mostly to double check with Little's formula that our measurements are ok
        u64 debt;
    };

    struct timer {
    private:
        std::chrono::high_resolution_clock::time_point start;

    public:
        const u32 tics_per_usec;
        const u64 tics_per_sec;
        timer(u32 _t) : tics_per_usec(_t / 1000000), tics_per_sec(_t) {
#define MIN_TICS_PER_SEC_       1000000000ULL // assume min 1GHz
            if (tics_per_sec < MIN_TICS_PER_SEC_) {
                fprintf(stderr, "tics_per_sec < 1GHz, please supply a correct value\n");
                assert(0);
            }
        };


        inline void start_t() {
            start = std::chrono::high_resolution_clock::now();
        }

        inline u64 stop_t() {
            using Unit  = std::chrono::nanoseconds;
            return std::chrono::duration_cast<Unit>(std::chrono::high_resolution_clock::now() - start).count();
        }

        inline u64 t_long_nsec() {
            auto now = std::chrono::system_clock::now();
            return std::chrono::time_point_cast<std::chrono::nanoseconds>(now).time_since_epoch().count();
        }

        inline u64 t_long_sec() {
            auto now = std::chrono::system_clock::now();
            return std::chrono::time_point_cast<std::chrono::seconds>(now).time_since_epoch().count();
        }

        inline u64 t_long_msec() {
            auto now = std::chrono::system_clock::now();
            return std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();
        }

        inline u64 t_long_usec() {
            auto now = std::chrono::system_clock::now();
            return std::chrono::time_point_cast<std::chrono::microseconds>(now).time_since_epoch().count();
        }

        inline std::chrono::high_resolution_clock::time_point t_point() {
            return std::chrono::high_resolution_clock::now();
        }

        inline u64 stop_t_milli() {
            using Unit  = std::chrono::milliseconds;
            return std::chrono::duration_cast<Unit>(std::chrono::high_resolution_clock::now() - start).count();
        }

        inline u64 ticks() {
            return ticks::get_ticks();
        }

    };

    struct xput_statistics {
        u64 offset;
        u64 end_curr_epoch; //Ticks
        u32 curr_epoch;
        u32 counter;
        struct timer *x_timer;
        std::vector<struct xput_sample> samples;
        u32 id;
        u32 tics_per_usec;
#ifdef USE_RESERVOIR
        std::vector<std::array<reservoir, OP_LAST>> latency_reservoirs;
        std::vector<std::array<t_reservoir<perc_s>, OP_LAST>> breakdown_reservoirs;
#endif

        ~xput_statistics() {
            delete x_timer;
        }


        xput_statistics(u32 _id, u32 _tics_per_usec) : id(_id) {
            x_timer = new timer(_tics_per_usec);
            // samples = (struct xput_sample *) malloc(sizeof(struct xput_sample) * XPUT_SAMPLES);
            // if (!samples) {
            //     FATAL("Could not allocate xput samples");
            // }
            // latency_reservoirs.resize(XPUT_SAMPLES);
            // breakdown_reservoirs.resize(XPUT_SAMPLES);
            reset_xput_stats();
        }

        void reset_xput_stats() {
            std::array<reservoir, OP_LAST> ar;
            std::array<t_reservoir<perc_s>, OP_LAST> atr;
            curr_epoch = 0;
            offset = x_timer->ticks();
            end_curr_epoch = x_timer->tics_per_sec;

            counter = 0;
            samples.push_back(xput_sample());
            latency_reservoirs.push_back(ar);
            breakdown_reservoirs.push_back(atr);
            samples[curr_epoch].ops = 0;
            samples[curr_epoch].time = 0;//0;//x_timer->t_long_sec();
            samples[curr_epoch].cumul = 0;
            samples[curr_epoch].debt = 0;
            latency_reservoirs[curr_epoch][OP_INSERT].reset();
            latency_reservoirs[curr_epoch][OP_GENERIC].reset();
            latency_reservoirs[curr_epoch][OP_UPDATE].reset();
            breakdown_reservoirs[curr_epoch][OP_GENERIC].reset();
        }

        inline void add_breakdown_sample(OPS op, unsigned long total, unsigned long begin, unsigned long body,
                                         unsigned long commit) {
            breakdown_reservoirs[curr_epoch][op].add(perc_s(total, begin, body, commit));
        }

        inline void add_sample(unsigned long latency, unsigned long init_time, OPS op) {
            /*
             * For simplicity, we assign the latency of an op to the epoch in which the operation started.
             * This means that there can be some mismatch between the number of samples and their values as
             * measured by perf and by this function, bc perf likely timestamps an operation with the completion time
             */

            samples[curr_epoch].ops++;
            samples[curr_epoch].cumul += latency;
            latency_reservoirs[curr_epoch][op].add(latency);
            const u64 now = x_timer->ticks();
            const u64 elapsed_ticks = now - offset;

            //We started an op in an epoch, and ended it in a following one.
            //The whole time is going to be charged to the starting epoch
            //We want to know how much time actually belongs to the other epochs we have crossed
            //just to double check that the max number of ticks per epoch is ticks_per_second
            if (elapsed_ticks >= end_curr_epoch) {
                samples[curr_epoch].debt = latency - end_curr_epoch;
            }

            TRACE_FORMAT("SAMPLE OPS[%u][%u] = %u (%lu).", id, curr_epoch, samples[curr_epoch].ops,
                         samples[curr_epoch].cumul);

            while (elapsed_ticks >= end_curr_epoch) {
                if (!id) {
                    PRINT_FORMAT("OPS[%u][%u] = %u (%lu).", id, samples[curr_epoch].time,
                                 samples[curr_epoch].ops, samples[curr_epoch].cumul);
                }
                curr_epoch++;
                std::array<reservoir, OP_LAST> ar;
                std::array<t_reservoir<perc_s>, OP_LAST> atr;
                samples.push_back(xput_sample());
                latency_reservoirs.push_back(ar);
                breakdown_reservoirs.push_back(atr);
                samples[curr_epoch].ops = 0;
                samples[curr_epoch].time = curr_epoch;//x_timer->t_long_sec();
                samples[curr_epoch].cumul = 0;
                samples[curr_epoch].debt = 0;
                latency_reservoirs[curr_epoch][OP_GENERIC];
                latency_reservoirs[curr_epoch][OP_INSERT];
                latency_reservoirs[curr_epoch][OP_INIT];
                latency_reservoirs[curr_epoch][OP_COMMIT];
                latency_reservoirs[curr_epoch][OP_UPDATE];
                breakdown_reservoirs[curr_epoch][OP_GENERIC];
                end_curr_epoch = (curr_epoch + 1) * x_timer->tics_per_sec;
            }
        }

        inline void add_sample(unsigned long latency, OPS op) {

            /*
             * For simplicity, we assign the latency of an op to the epoch in which the operation started.
             * This means that there can be some mismatch between the number of samples and their values as
             * measured by perf and by this function, bc perf likely timestamps an operation with the completion time
             * //FIXME @ddi ???
             */
            latency_reservoirs[curr_epoch][op].add(latency);
        }

    };


    struct fkvb_thread_state {

#define KEY_BUFFER_SIZE (1UL<<10)
#define VALUE_BUFFER_SIZE (1UL<<16)
        struct io_pattern *key_index_generator;
        struct io_pattern *scan_length_generator;
        struct io_pattern *batch_size_generator;
        struct io_pattern *value_size_generator;
        struct next_op_pattern *next_op_generator;
        struct next_op_pattern *secondary_next_op_generator;
        struct xput_statistics *xput_stats;

        u32 id, client_id;
        u64 duration;
        duration_type duration_t;
        u64 last_duration, last_init;
        u32 generic_ops;
        OPS last_op;

        prefix_key_string_builder *key_builder;
        value_string_builder_rnd *value_builder;

        size_t buffer_size_value = VALUE_BUFFER_SIZE;

        struct timer *op_timer;

        char key_buffer[KEY_BUFFER_SIZE]; //For gets, and start_key in scans
        char key_buffer2[KEY_BUFFER_SIZE]; //For end_key in scans
        char value_buffer[VALUE_BUFFER_SIZE];

        //Stores the keys for a bulk update
        char key_buffer_bulk[KEY_BUFFER_SIZE * BULK_SIZE];
        //Stores the values for a bulk update
        char value_buffer_bulk[VALUE_BUFFER_SIZE * BULK_SIZE];
        //Stores the pointers to the values for a bulk update.
        //Since we do not actually generate new values, these pointers point to addresses within the static value buffer
        //Unused for now
        char value_buffer_bulk_ptrs[BULK_SIZE];
        size_t key_sizes_bulk[BULK_SIZE];
        size_t value_sizes_bulk[BULK_SIZE];
        KVOrdered <IO> *kv;//ptr to the kv store

        u32 num_keys;//number of keys to insert upon load
        volatile bool running = true;

        char *generic_key_buffer, *generic_putvalue_buffer, *generic_getvalue_buffer;
        bool *generic_rw;
        size_t *generic_key_sizes, *generic_value_sizes;
        char **generic_put_ptr, **generic_get_ptr;


        fkvb_thread_state(u32 _id, u32 cid, u32 freq) : id(_id), client_id(cid) {
            op_timer = new timer(freq);
            xput_stats = new xput_statistics(id, freq);
        }


        ~fkvb_thread_state() {
            delete key_index_generator;
            delete scan_length_generator;
            delete batch_size_generator;
            delete value_size_generator;
            delete next_op_generator;
            delete op_timer;
            free(generic_key_buffer);
            free(generic_putvalue_buffer);
            free(generic_getvalue_buffer);
            free(generic_rw);
            free(generic_key_sizes);
#ifdef TRACE_PERF
            delete latency_reservoir;
#endif
        }

        void init_duration(std::string &dur) {
            unsigned bytes;
            unsigned count;
            const char *pattern = dur.c_str();
            if (dur.find("sec") != std::string::npos) {
                duration_t = duration_type::SEC_N;
                count = sscanf(pattern, "sec%lu%n", &duration, &bytes);
                if (count != 1 || bytes != strlen(pattern)) {
                    FATAL("Error in parsing second duration: %s (%u %u). Format is sec%%d\n",
                          pattern, bytes, count);
                }
            } else if (dur.find("ops") != std::string::npos) {
                duration_t = duration_type::OPS_N;
                count = sscanf(pattern, "ops%lu%n", &duration, &bytes);
                if (count != 1 || bytes != strlen(pattern)) {
                    FATAL("Error in parsing ops duration: %s (%u %u). Format is ops%%d\n",
                          pattern, bytes, count);
                }
            } else {
                //Should never get here: sanitize_params should take care of this case
                FATAL("Could not recognize operation type %s", dur.c_str());
            }
        }


        void start_op_timer() {
            op_timer->start_t();
        }

        u64 stop_op_timer() {
            return op_timer->stop_t();
        }

        inline void add_sample(unsigned long d, unsigned long l, OPS op) {
            xput_stats->add_sample(d, l, op);
        }

        inline void add_sample(unsigned long l, OPS op) {
            xput_stats->add_sample(l, op);
        }

        inline void add_breakdown_sample(OPS op, unsigned long t, unsigned long s, unsigned long b, unsigned long c) {
            xput_stats->add_breakdown_sample(op, t, s, b, c);
        }

        void dump_xputs(const char *file) {
            TRACE_FORMAT("DUMPING THREAD %u to %s", id, file);
            u32 i;
            std::ofstream myfile(file, std::fstream::app | std::fstream::out);
            if (myfile.fail()) {
                PRINT_FORMAT("Error opening %s", file);
                throw std::ios_base::failure(std::strerror(errno));
            }
            for (i = 0; i <= xput_stats->curr_epoch; i++) {
                xput_stats->latency_reservoirs[i][OP_INSERT].sort();
                xput_stats->latency_reservoirs[i][OP_GENERIC].sort();
                xput_stats->latency_reservoirs[i][OP_UPDATE].sort();
                reservoir &r_i = xput_stats->latency_reservoirs[i][OP_INSERT];
                reservoir &r_g = xput_stats->latency_reservoirs[i][OP_GENERIC];
                reservoir &r_u = xput_stats->latency_reservoirs[i][OP_UPDATE];

                xput_stats->latency_reservoirs[i][OP_INIT].sort();
                xput_stats->latency_reservoirs[i][OP_COMMIT].sort();
                reservoir &r_in = xput_stats->latency_reservoirs[i][OP_INIT];
                reservoir &r_co = xput_stats->latency_reservoirs[i][OP_COMMIT];
                t_reservoir<perc_s> &bg = xput_stats->breakdown_reservoirs[i][OP_GENERIC];
                bg.sort();

                perc_s p50 = bg.get_percentile(0.5);
                perc_s p99 = bg.get_percentile(0.99);

                myfile << id << " "
                       << xput_stats->samples[i].time << " "
                       << xput_stats->samples[i].ops << " "
                       << xput_stats->samples[i].cumul << " "
                       << xput_stats->samples[i].debt << " "
                       << r_i.get_percentile(0.5) << " "
                       << r_i.get_percentile(0.99) << " "
                       << r_g.get_percentile(0.5) << " "
                       << r_g.get_percentile(0.99) << " "
                       << r_in.get_avg() << " "
                       << r_in.get_percentile(0.5) << " "
                       << r_in.get_percentile(0.99) << " "
                       << r_co.get_avg() << " "
                       << r_co.get_percentile(0.5) << " "
                       << r_co.get_percentile(0.99) << " "
                       << r_u.get_avg() << " "
                       << r_u.get_percentile(0.5) << " "
                       << r_u.get_percentile(0.99) << " "
                       << p50.start << " "
                       << p50.commit << " "
                       << p50.total << " "
                       << p99.start << " "
                       << p99.commit << " "
                       << p99.total << "\n";

                TRACE_FORMAT("%u %u %u %lu %lu", id, xput_stats->samples[i].time, xput_stats->samples[i].ops,
                             xput_stats->samples[i].cumul, xput_stats->samples[i].debt);
                myfile.flush();
            }
            myfile.close();
            TRACE_FORMAT("DUMPED THREAD %u to %s", id, file);
        }
    };

    KVOrdered <IO> *kv;
    struct fkvb_thread_state **states;
    struct fkvb_thread_state **population_states;
    struct fkvb_test_conf *conf;

    static void init_state(struct fkvb_thread_state *state, fkvb_test_conf *conf, KVOrdered <IO> *kv, u32 id, long seed,
                           bool populate);

    static bool do_transaction(struct fkvb_thread_state *state);

    static int do_read(struct fkvb_thread_state *state);

    static int do_update(fkvb_thread_state *state);

    static int do_rmw(fkvb_thread_state *state);

    static int do_scan(fkvb_thread_state *state);

    static int do_insert(fkvb_thread_state *state);

    static int do_populate(fkvb_thread_state *state);

    static int do_generic(fkvb_thread_state *state);

    static int do_populate_bulk(fkvb_thread_state *state, u32 ops);

    static void *tx_loop(void *state);

    static void *populate_loop(void *state);

    void sigusr1_handler(int s);

    void sigusr2_handler(int s);

    void sigint_handler(int s);


public:
    FKVB(KVOrdered <IO> *kv, fkvb_test_conf *conf);

    void run();

    ~FKVB() {
        free(states);
    }
};


#endif //FKVB_HH
