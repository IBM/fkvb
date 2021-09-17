/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */
#include "FKVB.hh"
#include <stdlib.h>
#include "defs.hh"


//#define TH_AFFINITY
/*
#ifdef TH_AFFINITY
#pragma  message("Thread affinity is on")
#else
#pragma  message("Thread affinity is OFF")
#endif
*/
#define THREAD_ALWAYS_PRINT(format, arg...)                \
    do {                        \
        fprintf(stdout, "[%d][%d] %s:%d:  " format "\n",    \
            state->client_id,state->id, __FUNCTION__, __LINE__, ##arg);  fflush(stdout);\
    } while (0)

#ifdef PRODUCTION
#define THREAD_PRINT(format, arg...)
#define THREAD_TRACE(format, arg...)

#else //No production
#define THREAD_PRINT(format, arg...)                \
    do {                        \
        fprintf(stdout, "[%d][%d] %s:%d:  " format "\n",    \
            state->client_id,state->id, __FUNCTION__, __LINE__, ##arg); \
    } while (0)

#ifdef TRACE
#define THREAD_TRACE(format, arg...)                \
    do {                        \
        fprintf(stdout, "[%d] %s:%d:  " format "\n",    \
            state->id, __FUNCTION__, __LINE__, ##arg); \
    } while (0)
#else
#define THREAD_TRACE(format, arg...)
#endif

#endif//Production


#define START_TIMER(state)
#define END_TIMER(state)

struct io_pattern *init_rnd_gen(std::string *string, fkvb_test_conf *conf, long seed);

double compute_zipf_skew(int access_pct, int address_pct, unsigned long long n);

seed_t get_random_seed();

const char *op_to_string(OPS op);

void signal_barrier(const char *file);

static volatile bool failed = false;

bool populating = true;


u64 grv_cache_tics_ms=0;
thread_local uint64_t zrl_fkvb_begin_latency = 0, zrl_fkvb_commit_latency = 0;
thread_local u32 tid;
u32 sleep_time_us=0;

template<typename IO>
FKVB<IO>::FKVB(KVOrdered <IO> *_kv, fkvb_test_conf *con) : kv(_kv), conf(con) {
    u32 t = 0;
    population_barrier = conf->load_barrier;

    states = (struct fkvb_thread_state **) malloc(
            sizeof(fkvb_thread_state *) * conf->thread_nr_m);

    if (nullptr == states) {
        fprintf(stdout, "Error allocating the fkvb_thread_states. Exit\n");
        exit(-1);
    }

    population_states = (struct fkvb_thread_state **) malloc(
            sizeof(fkvb_thread_state *) * conf->num_population_threads);

    if (nullptr == population_states) {
        fprintf(stdout, "Error allocating the population fkvb_thread_states. Exit\n");
        exit(-1);
    }

    grv_cache_tics_ms = conf->grv_cache_ms * (conf->frequency / 1000);
    sleep_time_us = conf->sleep_time_us;
    fprintf(stdout,"grv_cache_tics %lu\n",grv_cache_tics_ms);
    long seed = conf->seed;
    const bool is_population_thread = true;
    rand_io_pattern seed_generator = rand_io_pattern(1000000000, 0, seed);

    for (t = 0; t < conf->thread_nr_m; t++) {
        states[t] = new fkvb_thread_state(t, con->instance_id, con->frequency);
        init_state(states[t], con, kv, t, (long) seed_generator.next(), !is_population_thread);
    }

    for (t = 0; t < conf->num_population_threads; t++) {
        population_states[t] = new fkvb_thread_state(t, con->instance_id, con->frequency);
        init_state(population_states[t], con, kv, t, (long) seed_generator.next(), is_population_thread);
    }
}

template<typename IO>
int FKVB<IO>::do_read(fkvb_thread_state *state) {
    THREAD_TRACE("%s", "Doing a read");
    size_t key_size, val_size_read, val_size;
    const size_t val_buffer_size = state->buffer_size_value;
    char *key = state->key_buffer;
    char *value = state->value_buffer;
    int rc;
    //Pick key number
    const int next_key_index = state->key_index_generator->next();
    THREAD_TRACE("Next key index %u", next_key_index);


    //Pick actual key
    state->key_builder->build(next_key_index, key, &key_size);
    THREAD_TRACE("Next key %s", key);
    //do op
    START_TIMER(state);
    Y_PROBE_TICKS_START(do_read);
    rc = state->kv->get(key, key_size, value, val_buffer_size, val_size_read, val_size);
    Y_PROBE_TICKS_END(do_read);
    END_TIMER(state);
    THREAD_TRACE("Read value %s. Time taken %lu nsec", value, time);
    return rc;

}


template<typename IO>
int FKVB<IO>::do_scan(fkvb_thread_state *state) {
    size_t key_size, val_size_read;
    const size_t val_buffer_size = state->buffer_size_value;

    char *start_key = state->key_buffer, *end_key = state->key_buffer2;
    char *values = state->value_buffer;
    int rc;
    size_t scan_length = state->scan_length_generator->next();

    //Pick key number
    const int next_key_index = state->key_index_generator->next();
    const int end_key_index = next_key_index + scan_length;

    char *beg, *end;

    //Pick actual key
    state->key_builder->build(next_key_index, start_key, &key_size);
    state->key_builder->build(end_key_index, end_key, &key_size);

    if (strcmp(start_key, end_key) > 0) {
        beg = end_key;
        end = start_key;
    } else {
        end = end_key;
        beg = start_key;
    }

    THREAD_TRACE("Range: [%s, %s]", beg, end);
    //do op
    START_TIMER(state);
    std::vector<char *> kv_ptrs = std::vector<char *>();

    Y_PROBE_TICKS_START(do_range);
    rc = state->kv->get_range(beg, key_size, end, key_size, values,
                              val_buffer_size, val_size_read, kv_ptrs);
    Y_PROBE_TICKS_END(do_range);
    END_TIMER
    (state);
    THREAD_TRACE("Scan done. Time taken %"
                         P64
                         " nsec", time);
    return rc;
}


template<typename IO>
/*
 * Right now the easiest way to plug this in is to just pass the keys we want to read and write
 * and let the kv define a proper operation.
 * This means we cannot have a proper business logic in the tx: we just do operations, but that's not a big deal now
 */
int FKVB<IO>::do_generic(fkvb_thread_state *state) {
    u32 op = 0;
    u32 curr_w = 0;
    char *curr_key = state->generic_key_buffer, *curr_put_value = state->generic_putvalue_buffer;
    size_t *curr_key_size = state->generic_key_sizes, *curr_value_size = state->generic_value_sizes;
    bool *rw = state->generic_rw;

    const size_t value_sizes = state->value_builder->next_size();
    while (op < state->generic_ops) {
        const int next_key_index = state->key_index_generator->next();

        //Pick actual key
        state->key_builder->build(next_key_index, curr_key, &curr_key_size[op]);
        THREAD_TRACE("Next key index %u: %.*s", next_key_index, (int) curr_key_size[op], curr_key);
        curr_key += curr_key_size[op];


        if (state->secondary_next_op_generator->next() == OP_UPDATE) { //Pick value to put
            rw[op] = true;
            curr_put_value = (char *) state->value_builder->_build(&curr_value_size[curr_w]);
            state->generic_put_ptr[curr_w] = curr_put_value;
            //curr_put_value += curr_value_size[curr_w];
            THREAD_TRACE("Generic put on key %.*s: (%p) value %.*s length %zu",
                         (int) curr_key_size[op], (curr_key - curr_key_size[op]),
                         curr_put_value, (int) curr_value_size[curr_w],
                         state->generic_put_ptr[curr_w], curr_value_size[curr_w]);
            curr_w++;
        } else {
            rw[op] = false;
        }
        op++;
    }
    std::vector<char *> vect;
    size_t get_buff_size = value_sizes * state->generic_ops;
    size_t size_read;
    int rc;

    Y_PROBE_TICKS_START(do_generic);
    rc = state->kv->generic(state->generic_ops, state->generic_rw, state->generic_key_buffer,
                            state->generic_key_sizes,
                            state->generic_put_ptr, state->generic_value_sizes,
                            state->generic_getvalue_buffer, get_buff_size, &size_read, vect);
    Y_PROBE_TICKS_END(do_generic);
    return rc;
}

template<typename IO>
int FKVB<IO>::do_update(fkvb_thread_state *state) {
    THREAD_TRACE("%s", "Doing an update");
    const char *value_ptr;
    size_t key_size;
    int rc;
    char *key = state->key_buffer;
    //Pick key number
    const int next_key_index = state->key_index_generator->next();
    THREAD_TRACE("Next key index %u", next_key_index);

    //Pick actual key
    state->key_builder->build(next_key_index, key, &key_size);
    THREAD_TRACE("Next key %s", key);

    //Pick value
#if 0
    state->value_builder->build(state->value_buffer, &state->buffer_size_value);
    value_ptr=state->value_buffer;
#else
    value_ptr = state->value_builder->_build(&state->buffer_size_value);
#endif
    //do op
    START_TIMER(state);
    Y_PROBE_TICKS_START(do_update);
    rc = state->kv->put(key, key_size, value_ptr, state->buffer_size_value);
    Y_PROBE_TICKS_END(do_update);
    END_TIMER(state);
    return rc;

}

template<typename IO>
int FKVB<IO>::do_insert(fkvb_thread_state *state) {
    THREAD_TRACE("%s", "Doing an insert (update)");
    size_t key_size;
    const char *value_ptr;
    int rc;
    char *key = state->key_buffer;
    //Pick key number
    const int next_key_index = state->key_index_generator->next();
    THREAD_TRACE("Next key index %u", next_key_index);

    //Pick actual key
    state->key_builder->build(next_key_index, key, &key_size);
    THREAD_TRACE("Next key %s", key);

    //Pick value
#if 0
    state->value_builder->build(state->value_buffer, &state->buffer_size_value);
    value_ptr=state->value_buffer;
#else
    value_ptr = state->value_builder->_build(&state->buffer_size_value);
#endif
    //do op
    START_TIMER(state);
    Y_PROBE_TICKS_START(do_insert);
    rc = state->kv->put(key, key_size, value_ptr, state->buffer_size_value);
    Y_PROBE_TICKS_END(do_insert);
    END_TIMER(state);
    THREAD_TRACE("Written %s. Time taken %"
                         P64
                         " nsec", state->value_buffer, time);
    return rc;

}


template<typename IO>
int FKVB<IO>::do_populate_bulk(fkvb_thread_state *state, u32 ops) {
    unsigned int i;
    int rc;
    int next_key_index;
    char *key = state->key_buffer_bulk;
    char *value_ptr = state->value_buffer_bulk;
    tid = state->id;
    for (i = 0; i < ops; i++) {
        next_key_index = state->key_index_generator->next();
        state->key_builder->build(next_key_index, key, &state->key_sizes_bulk[i]);
        THREAD_TRACE("POP: NEXT KEY %u  %.*s %d", next_key_index, (int) (state->key_sizes_bulk[i]), key,
                     (int) (state->key_sizes_bulk[i]));
        key += state->key_sizes_bulk[i];
        state->value_builder->build(value_ptr, &state->value_sizes_bulk[i]);
        value_ptr += state->value_sizes_bulk[i];
    }
    START_TIMER(state);
    Y_PROBE_TICKS_START(do_populate);
    rc = state->kv->put_bulk((size_t) BULK_SIZE, state->key_buffer_bulk, &state->key_sizes_bulk[0],
                             state->value_buffer_bulk, &state->value_sizes_bulk[0]);
    Y_PROBE_TICKS_END(do_populate);
    END_TIMER(state);
    return rc;
}

template<typename IO>
int FKVB<IO>::do_populate(fkvb_thread_state *state) {
    THREAD_TRACE("%s", "Doing an insert (update)");
    size_t key_size;
    tid = state->id;
    const char *value_ptr;
    int rc;
    char *key = state->key_buffer;
    //Pick key number
    const int next_key_index = state->key_index_generator->next();
    THREAD_TRACE("Next key index %u", next_key_index);

    //Pick actual key
    state->key_builder->build(next_key_index, key, &key_size);
    THREAD_TRACE("Next key %s", key);

    //Pick value
#if 0
    state->value_builder->build(state->value_buffer, &state->buffer_size_value);
    value_ptr=state->value_buffer;
#else
    value_ptr = state->value_builder->_build(&state->buffer_size_value);
#endif
    //do op
    START_TIMER(state);
    Y_PROBE_TICKS_START(do_populate);
    rc = state->kv->put(key, key_size, value_ptr, state->buffer_size_value);
    Y_PROBE_TICKS_END(do_populate);
    END_TIMER(state);
    THREAD_TRACE("Written %s. Time taken %"
                         P64
                         " nsec", state->value_buffer, time);
    return rc;

}

template<typename IO>
int FKVB<IO>::do_rmw(fkvb_thread_state *state) {
    THREAD_TRACE("%s", "Doing a rmw");
    size_t key_size, val_size_read, val_size;
    int rc;
    const size_t val_buffer_size = state->buffer_size_value;
    char *key = state->key_buffer;
    char *value = state->value_buffer; //Store the value of the read
    //Pick key number
    const int next_key_index = state->key_index_generator->next();
    THREAD_TRACE("Next key index %u", next_key_index);

    //Pick actual key
    state->key_builder->build(next_key_index, key, &key_size);
    THREAD_TRACE("Next key %s", key);

    rc = state->kv->get(key, key_size, value, val_buffer_size, val_size_read, val_size);
    if (rc) {
        return rc;
    }
    //Pick value
    value = state->value_buffer;
    //do op
    START_TIMER(state);
    rc = state->kv->put(key, key_size, value, val_buffer_size);
    END_TIMER(state);
    THREAD_TRACE("RMW New value %s. Time taken %"
                         P64
                         " nsec", value, time);

    return rc;

}


#include <bitset>

template<typename IO>
void
FKVB<IO>::init_state(struct fkvb_thread_state *state, fkvb_test_conf *conf, KVOrdered <IO> *kv, u32 id, long seed,
                     bool populate) {

    state->id = id;
    state->num_keys = conf->num_keys;
    THREAD_TRACE("Initing %s thread at address %p with %u keys and seed %ld", populate ? "population" : "runtime",
                 state, state->num_keys, seed);


    rand_io_pattern rnd = rand_io_pattern((size_t) 123321456654789987ULL, 0, seed);


    //Data access pattern
    if (!populate) {
        int stagger = (1 + conf->instance_id) * conf->thread_nr_m + state->id;
        while (stagger--) {
            rnd.next();
        }
        long _seed = (long) rnd.next() + seed;
        printf("Thread %u on client %u has next_key_seed %lu\n", state->id, conf->instance_id, _seed);
        state->key_index_generator = init_rnd_gen(&conf->key_gen, conf, _seed);
    }

    else {
        //Find out how many keys to this id
        if (conf->key_type.find("sharded") != std::string::npos) {
            //If keys are sharded, then the num_keys is the total amount of keys
            state->key_index_generator = new seq_io_pattern(state->num_keys, 0, 0);
        } else {
            //If keys are in common across clients, determine which keys has to be populated by whom
            u32 rem = state->num_keys % conf->num_instances;
            u32 key_to_instance = (state->num_keys - rem) / conf->num_instances;
            if (!state->id) { printf("Num instances %u remainder %u\n", conf->num_instances, rem); }
            u32 lowest_key_instance = key_to_instance * conf->instance_id; //inclusive
            u32 highest_key_instance = lowest_key_instance + key_to_instance; //exclusive

            if (conf->instance_id == (conf->num_instances - 1)) { //last instance takes a bit more
                key_to_instance += rem;
            }
            if (!state->id && conf->instance_id == (conf->num_instances - 1)) {
                printf("Total keys %u. Total instances %u. Keys for this instance %u (id = %u). Lowest %u. Highest %u\n",
                       state->num_keys, conf->num_instances, key_to_instance, conf->instance_id, lowest_key_instance,
                       highest_key_instance);
            }

            //Now find out which keys per thread
            unsigned int remainder = key_to_instance % conf->num_population_threads;
            unsigned int keys_per_thread = key_to_instance / conf->num_population_threads;
            if (1) {
                printf("Keys per thread %"
                                     P8
                                     " are %u remainder %u", id, keys_per_thread, remainder);
            }
            //PRINT_FORMAT("%u %u %u %u", keys_per_thread, remainder,
            //             remainder + keys_per_thread * conf->num_population_threads, state->num_keys);
            assert(remainder + keys_per_thread * conf->num_population_threads == state->num_keys);
            unsigned int low = lowest_key_instance + state->id * keys_per_thread;
            unsigned int high = low + keys_per_thread +
                                (id == (conf->num_population_threads - 1) ? remainder : 0); //last thread gets more
            state->key_index_generator = new seq_io_pattern(high, low, low);
        }
    }

    if (conf->key_type == "fkvb") {
        if (!state->id)PRINT_FORMAT("\n------  PREFIX KEYS v-------\n");
        state->key_builder = new prefix_key_string_builder(nullptr, (size_t) conf->key_size, conf->num_keys);
    } else if (conf->key_type == "random") {
        if (!state->id) PRINT_FORMAT("\n------  RND KEYS v-------\n");
        state->key_builder = new key_string_builder_rnd(new rand_io_pattern(250, 0), (size_t) conf->key_size,
                                                        conf->num_keys);
    } else {

        int th = populate ? conf->num_population_threads : conf->thread_nr_m;
        unsigned int tid = (1 + conf->instance_id) * th + state->id;
        rand_io_pattern shard_rnd = rand_io_pattern(100000000000, 0, seed);
        int count = tid;
        while (count--) {
            shard_rnd.next();
        }
        //We use a huge max number so that all 8 bytes are going (likely) to be non 0
        //With a lower max, FDB was failing bc of some weird keys
        //FIXME: FDB does not allow user keys starting with \xff\xff
        size_t shard;
        do {
            unsigned long shard1 = rand_io_pattern(1ULL << 63, 0, shard_rnd.next()).next();
            unsigned long shard2 = rand_io_pattern(1ULL << 63, 0, shard_rnd.next()).next();
            //We concatenate the highest 32 bits (LE) coming from two rnd numbers
            //This is to try to avoid two zero octects at the beginning of the string, which mess up naive printing
            unsigned long mask=0xFFFFFFFF00000000;
            //std::cout << mask << " = " << std::bitset<64>(mask) << std::endl;
            shard = ((shard1 & mask) | ((shard2 & mask) >> 32));
        } while (((unsigned char *) &shard)[0] == 255);

        // assert(tid <= ((1UL << 16) - 1));//Be sure it's less than 2 bytes
        assert(sizeof(size_t) == 8);
        //size_t shard = shard_rnd.next();
        //Don't care about endianess
        printf("Thread %u on client %u has shard %lu with init seed %lu\n", state->id, conf->instance_id, shard, seed);
        std::string ref = std::string((char *) &shard, sizeof(size_t));
        if (conf->key_type == "sharded_fkvb") {
            if (!state->id) PRINT_FORMAT("\n------  SHARDED FKVB  KEYS v-------\n");
            state->key_builder = new prefix_key_string_builder(nullptr, (size_t) conf->key_size, conf->num_keys, ref);
        } else {
            if (!state->id) PRINT_FORMAT("\n------  SHARDED  RND KEYS v-------\n");
            state->key_builder = new prefix_key_string_builder_rnd(new rand_io_pattern(250, 0), (size_t) conf->key_size,
                                                                   conf->num_keys, ref);
        }
#if 1
        printf("SHARD: %zu\n", shard);
        std::cout << "c = " << std::bitset<64>(shard) << std::endl;
        unsigned s = 0;

        for (; s < sizeof(size_t); s++) {
            unsigned char c = (unsigned char) (ref.c_str()[s]);
            printf("%*.s %d = %hu %c\n", (int) sizeof(size_t), ref.c_str(), s, c, c);
        }
        size_t ss;
        assert(conf->key_size < 4096);
        char t[4096];
        for (s = 0; s < 10; s++) {
            unsigned cc;
            state->key_builder->build(s, t, &ss);
            printf("%u %u = %.*s\n", conf->instance_id, state->id, conf->key_size, t);
            for (cc = 0; cc < conf->key_size; cc++) {
                unsigned char c = (unsigned char) (t[cc]);
                printf("%hu.", c);
            }
            printf("\n");
        }
#endif
    }

    //Operations: generator
    state->next_op_generator = new next_op_pattern((long) rnd.next());
    //Operations: percentages
    if (conf->scan_perc) {
        state->scan_length_generator = init_rnd_gen(&conf->scan_len_gen, conf, (long) rnd.next());
        if (state->scan_length_generator == nullptr) {
            exit(1);
        }
        TRACE_FORMAT("Adding scan perc to generator %d", conf->scan_perc);
        state->next_op_generator->add_ops(OP_SCAN, conf->scan_perc);
    }
    if (conf->read_perc) {
        TRACE_FORMAT("Adding read perc to generator %"
                             P8, conf->read_perc);
        state->next_op_generator->add_ops(OP_READ, conf->read_perc);
    }
    if (conf->update_perc) {
        TRACE_FORMAT("Adding update perc to generator %"
                             P8, conf->update_perc);
        state->next_op_generator->add_ops(OP_UPDATE, conf->update_perc);
    }
    if (conf->rmw_perc) {
        TRACE_FORMAT("Adding rmw perc to generator %"
                             P8, conf->update_perc);
        state->next_op_generator->add_ops(OP_RMW, conf->rmw_perc);
    }
    if (conf->insert_perc) {
        FATAL("Insert operation is not currently supported.");
        TRACE_FORMAT("Adding insert perc to generator %"
                             P8, conf->insert_perc);
        state->next_op_generator->add_ops(OP_RMW, conf->insert_perc);
    }

    //Value
    state->value_builder = new value_string_builder_rnd(init_rnd_gen(&conf->value_size_gen, conf, seed));
    //TODO: Add a check such that the largest value fits within the buffer

    //KV
    state->kv = kv;
    //Buffers
    strcpy(state->value_buffer, "dummyValue");

    //Duration
    state->init_duration(conf->duration);

    //TO BE DONE AFTER INITIALIZING THE VALUE BUILDER, BC  WE NEED THAT SIZE
    if (conf->generic_perc) {
        if (state->value_builder->_pattern->_beg != state->value_builder->_pattern->_end) {
            FATAL("Right now, generic ops only work with values of constant size.");
            //Can we get around this by simply using the "_end" size to (over)provision the buffers?
        }
        state->next_op_generator->add_ops(OP_GENERIC, conf->generic_perc);
        state->secondary_next_op_generator = new next_op_pattern((long) rnd.next());
        state->secondary_next_op_generator->add_ops(OP_READ, conf->generic_rp);
        state->secondary_next_op_generator->add_ops(OP_UPDATE, 100 - conf->generic_rp);
        state->generic_ops = conf->generic_ops;
        TRACE_FORMAT("Adding generic perc to generator %"
                             P8
                             " with %"
                             P8
                             " read percentage", conf->generic_perc, conf->generic_rp);
        state->generic_key_buffer = (char *) malloc(state->generic_ops * conf->key_size);
        state->generic_putvalue_buffer = (char *) malloc(state->generic_ops * state->value_builder->next_size());
        state->generic_getvalue_buffer = (char *) malloc(state->generic_ops * state->value_builder->next_size());
        state->generic_rw = (bool *) malloc(state->generic_ops * sizeof(bool));
        state->generic_key_sizes = (size_t *) malloc(state->generic_ops * sizeof(size_t));
        state->generic_value_sizes = (size_t *) malloc(state->generic_ops * sizeof(size_t));
        state->generic_get_ptr = (char **) malloc(state->generic_ops * sizeof(char *));
        state->generic_put_ptr = (char **) malloc(state->generic_ops * sizeof(char *));

        if (state->generic_key_buffer == nullptr || state->generic_putvalue_buffer == nullptr ||
            state->generic_getvalue_buffer == nullptr || state->generic_rw == nullptr ||
            state->generic_key_sizes == nullptr || state->generic_value_sizes == nullptr ||
            state->generic_put_ptr == nullptr || state->generic_get_ptr == nullptr) {
            FATAL("Error in building buffers for generic ops");
        }
    }

}

//TODO: numKeys is only needed in one case.
//Refactor this to take low and high.
struct io_pattern *init_rnd_gen(std::string *string, fkvb_test_conf *conf, long seed) {
    size_t num_keys = (size_t) conf->num_keys;
    TRACE_FORMAT("%s", string->c_str());
    if (0 == string->compare(UNIFORM) &&
        strlen(string->c_str()) == strlen(UNIFORM)) { //Exact match: this is the DAP. Param is #keys
        TRACE_FORMAT("Uniform DAP over %u keys\n", conf->num_keys);
        return new rand_io_pattern(num_keys, 0, seed);
    } else if (0 == string->compare(0, strlen(UNIFORM), UNIFORM)) {//Substring match: params need to be parsed
        TRACE_FORMAT("%s\n", string->c_str());
        unsigned int lower = 0;
        unsigned int higher = 0;
        unsigned bytes = 0;
        //Last argument is the number of characters read
        const char *pattern = string->c_str();
        unsigned count = sscanf(pattern, UNIFORM
                                         "%u_%u%n",
                                &lower, &higher, &bytes);
        if (count != 2 || bytes != strlen(pattern)) {
            FATAL("Error in parsing a bounded uniform distr: %s (%u %u %u %u). Format is %s%%d_%%d\n",
                  pattern, lower, higher, bytes, count, UNIFORM);
            return nullptr;
        } else {
            if (lower > higher) {
                FATAL("Pattern %s has min  > max", pattern);
            }
            TRACE_FORMAT("Bounded uniform distr: %s with params %u %u", pattern, lower, higher);
            return new rand_io_pattern(lower, higher, seed);
        }
    } else if (0 == string->compare(0, strlen(ZIPFIAN), ZIPFIAN)) {//Zipfian is only for the dap. #key param is implicit
        unsigned int hot = 0;
        unsigned int perc = 0;
        unsigned bytes = 0;
        const char *pattern = string->c_str();
        unsigned count = sscanf(pattern, ZIPFIAN "%u_%u%n",
                                &hot, &perc, &bytes);
        if (count != 2 || bytes != strlen(pattern)) {
            FATAL("Error in parsing a bounded zipfian distr: %s (%u %u %u %u). Format is %s%%d_%%d\n",
                  pattern, hot, perc, bytes, count, ZIPFIAN);
            return nullptr;
        } else {
            TRACE_FORMAT("Bounded zipf distr: %s with params %u %u over %u keys\n", pattern, hot, perc,
                         conf->num_keys);
            double zipf = compute_zipf_skew(perc, hot, conf->num_keys);
            TRACE_FORMAT("Zipfian skew %f", zipf);
            return new zipf_io_pattern(conf->num_keys, 0, zipf, seed);
        }
    } else if (0 == string->compare(0, strlen(CONSTANT), CONSTANT)) {
        TRACE_FORMAT("%s", string->c_str());
        unsigned int lower = 0;
        unsigned bytes = 0;
        //Last argument is the number of characters read
        const char *pattern = string->c_str();
        unsigned count = sscanf(pattern, CONSTANT
                                         "%u%n",
                                &lower, &bytes);
        if (count != 1 || bytes != strlen(pattern)) {
            FATAL("Error in parsing a constant distr: %s (%u %u %u). Format is %s%%d\n",
                  pattern, lower, bytes, count,
                  CONSTANT);
            return nullptr;
        } else {
            TRACE_FORMAT("Constant distr: %s with param %u", pattern, lower);
            return new const_io_pattern(lower, lower);
        }
    } else {
        FATAL("Doing nothing for pattern %s", string->c_str());
    }
}

template<typename IO>
void *FKVB<IO>::tx_loop(void *_state) {

    //TODO SET A BARRIER
    usleep(3 * 1000000); //Allow for thread migration before resetting the clocks


    fkvb_thread_state *state = static_cast<fkvb_thread_state *> (_state);
    state->kv->thread_local_entry();
    state->xput_stats->reset_xput_stats();
    tid = state->id;
    u64 remaining = state->duration;
    switch (state->duration_t) {


        case duration_type::OPS_N: {
            if (0 == state->id) {
                THREAD_PRINT("Each thread is going to do %lu ops", remaining);
            }
            while (state->running && remaining-- && !failed) {
                do_transaction(state);
                state->add_sample(state->last_duration, state->last_init, state->last_op);
                state->add_sample(zrl_fkvb_begin_latency, OP_INIT);
                state->add_sample(zrl_fkvb_commit_latency, OP_COMMIT);
                if (!(remaining % 5000) && !state->id) {
                    THREAD_PRINT("Remaining %lu", remaining);
                }
            }
            break;
        }
        case duration_type::SEC_N: {

            u64 last = state->op_timer->t_long_usec();
            u64 init_time = last;
            if (0 == state->id) {
                THREAD_PRINT("Each thread is going to run for %lu seconds", remaining);
                THREAD_PRINT("INIT TIME %lu", last);
            }
            u64 now, end = init_time + state->duration * 1000000, last_print = init_time;
            u64 last_start=0;
            u64 last_taken=0;
            while (state->running && ((now = state->op_timer->t_long_usec()) < end) && !failed) {
                if(((last_taken=now-last_start)<sleep_time_us)){
                        usleep(sleep_time_us);// - last_taken);
                }    
                last_start=now;
                do_transaction(state);
                state->add_sample(state->last_duration, state->last_init, state->last_op);
                state->add_sample(zrl_fkvb_begin_latency, OP_INIT);
                state->add_sample(zrl_fkvb_commit_latency, OP_COMMIT);
                if (state->last_op == OP_GENERIC) {
                    state->add_breakdown_sample(OP_GENERIC, state->last_duration,
                                                state->last_duration -
                                                (zrl_fkvb_begin_latency + zrl_fkvb_commit_latency),
                                                zrl_fkvb_begin_latency,
                                                zrl_fkvb_commit_latency);
                }
                if (((now - last) > 1000000) && !state->id) {
                    THREAD_PRINT("%lu Remaining %lu sec", now - init_time, (end - now) / 1000000);
                    last = now;
                    if (((now - last_print) / 1000000) > 60) {
                        state->kv->print_stats();
                        last_print = now;
                    }
                }
            }
            break;
        }
        default: {
            assert(false);
        }
    }
    state->kv->thread_local_exit();
    pthread_exit(NULL);
}

template<typename IO>
void *FKVB<IO>::populate_loop(void *_state) {
    fkvb_thread_state *state = (fkvb_thread_state *) _state;
    state->kv->thread_local_entry();
    state->xput_stats->reset_xput_stats();
    u32 ops = state->key_index_generator->_end - state->key_index_generator->_beg;
    unsigned int i, rc, rollout, stats, done = 0, print_stats = 0;
    state->last_op = OP_INSERT;
    UNUSED(i);
    //We want to print advancement every time we insert 1% of the total keys
    rollout = (int) (((double) ops) * 0.01);
    rollout += 500;
    rollout = rollout / 1000;
    rollout *= 1000;
    if (!rollout) {
        if (ops < 100) {
            rollout = 10;
        } else if (ops < 1000) {
            rollout = 100;
        } else {
            rollout = 1000;
        }
    }
    stats = rollout * 10;  //10% when using 0.01 for rollout
    UNUSED(stats);
    if (!state->id)THREAD_PRINT("Population: ops per thread  %u. Rollout every %d", ops, rollout);

    while (ops && state->running && !failed) {

#if BULK_SIZE > 1
        u32 dd = ops > BULK_SIZE ? BULK_SIZE : ops;
        rc = do_populate_bulk(state, dd);
        if (rc) {
            FATAL("POPULATION BULK WRITE FAILED");
        }
//We need that #samples is the number of keys so that we can double check the load phase
//So we add a sample for each key written. The time of an op in a bulk insert is the time of the bulk insert
//Divided by the number of keys written
        for (i = 0; i < dd; i++) {
            //NOTE: this is done within a loop. This means that one op can belong
            //To one epoch, and one to another epoch (if the bulk happens across two epochs)
            //This is why sometimes we get odd values as xput in loading ;)
            state->add_sample(state->last_duration / dd, state->last_init, state->last_op);
        }

        ops -= dd;
        done += dd;
#else
        rc = do_populate(state);
        if(rc){
            FATAL("POPULATION WRITE FAILED");
        }
        state->add_sample(state->last_duration, state->last_init, state->last_op);
        ops--;
#endif
        if (!state->id) {
            while (done >= rollout) {
                THREAD_PRINT("Population: remaining (per thread) %u", ops);
                done -= rollout;
                print_stats++;
                if (print_stats == 10) {
                    THREAD_PRINT("Population: info");
                    state->kv->print_stats();
                    print_stats = 0;
                }

            }
        }
        /*
        if ((!(ops % rollout)) && (!state->id)) {//FIXME: this is only printed if rollout is a multiple of bulk_size...
            THREAD_PRINT("Population: remaining (per thread) %u", ops);
        }
        if ((!(ops % stats)) && (!state->id)) {
            THREAD_PRINT("Population: info");
            state->kv->print_stats();
        }*/
    }
    state->kv->thread_local_exit();
    pthread_exit(NULL);
}

template<typename IO>
bool FKVB<IO>::do_transaction(fkvb_thread_state *state) {
    OPS next_op = state->next_op_generator->next();
    state->last_op = next_op;
    THREAD_TRACE("Next op is %s", op_to_string(next_op));
    int rc;
    switch (next_op) {
        case OP_READ:
            rc = do_read(state);
            if (rc) {
                ERROR("DO_READ FAILED with code %d", rc);
                failed = true;
            }
            break;

        case OP_UPDATE: {

#define RETRY_BACKOFF

#ifndef RETRY_BACKOFF
            rc = do_update(state);
                if (rc) {
                    ERROR("DO_UPDATE FAILED with code %d",rd);
                    failed = true;
                }
                break;
#else
            size_t max_retries = 18; //2^18 msec. In total, roughly 10 minute worth of retrying
            size_t sleep_time = 1000; //in usec
            do {
                rc = do_update(state);
                if (rc) {
                    ERROR("DO_UPDATE FAILED with code %d . Sleeping %zu usec", rc, sleep_time);
                    usleep(sleep_time);
                    sleep_time *= 2;
                } else {
                    break;
                }
            } while (--max_retries);
            if (!max_retries) {
                ERROR("DO_UPDATE FAILED TOO MANY TIMES. FAILING THE TEST.");
                failed = true;
            }
            break;

#endif
        }

        case OP_INSERT:
            rc = do_insert(state);
            if (rc) {
                ERROR("DO_INSERT FAILED with code %d", rc);
                failed = true;
            }
            break;
        case OP_RMW:
            rc = do_rmw(state);
            if (rc) {
                FATAL("DO_RMW FAILED with code %d", rc);
                failed = true;
            }
            break;
        case OP_SCAN:
            rc = do_scan(state);
            if (rc) {
                FATAL("DO_SCAN FAILED with code %d", rc);
                failed = true;
            }
            break;
        case OP_GENERIC:
            rc = do_generic(state);
            if (rc) {
                ERROR("DO_GENERIC FAILED");
                failed = true;
            }
            break;
        case OP_INIT:
        case OP_COMMIT:
            FATAL("Unexpected next operation %d", next_op);
        default:
            FATAL("Operation not recognized %d", next_op);
    }

    return true;
}

template<typename IO>
void FKVB<IO>::sigusr1_handler(int s) {
    unsigned i;
    for (i = 0; i < conf->thread_nr_m; i++) {
        population_states[i]->running = false;
    }
    for (i = 0; i < conf->thread_nr_m; i++) {
        states[i]->running = false;
    }
}

template<typename IO>
void FKVB<IO>::sigusr2_handler(int s) {
    PRINT("RECEIVED SIGUSR2");
    population_barrier = false;
}


template<typename IO>
void FKVB<IO>::sigint_handler(int s) {
    PRINT("RECEIVED SIGINT");
    //Shortcut
    failed = true;
}


void signal_barrier(const char *file) {
    PRINT_FORMAT("WRITING BARRIER FILE %s", file);
    std::ofstream myfile(file, std::fstream::out | std::fstream::trunc);
    if (myfile.fail()) {
        PRINT_FORMAT("Error opening %s", file);
        throw std::ios_base::failure(std::strerror(errno));
    }
    myfile.close();
}

template<typename IO>
void FKVB<IO>::run() {

    unsigned i;
#if 0
    for (i = 1; i <= 10; i++) {
        uint64_t t = ticks::get_ticks();
        usleep(i * 1000000);
        t = ticks::get_ticks() - t;
        PRINT_FORMAT("(%u sec) TICS PER SEC %f", i, (double) t / (double) i);
        if ((i + 1) % 2)i++;
    }
#endif


    //Create the files used for xput output
    std::stringstream ss1;
    ss1 << conf->xput_file.c_str() << ".loadxput";
    std::ofstream myfile1(ss1.str().c_str(), std::fstream::out | std::fstream::trunc);
    if (myfile1.fail()) {
        ERROR("Error opening %s", ss1.str().c_str());
        throw std::ios_base::failure(std::strerror(errno));
    }
    myfile1.close();

    std::stringstream ss2;
    ss2 << conf->xput_file.c_str() << ".runxput";
    std::ofstream myfile2(ss2.str().c_str(), std::fstream::out | std::fstream::trunc);
    if (myfile2.fail()) {
        ERROR("Error opening %s", ss2.str().c_str());
        throw std::ios_base::failure(std::strerror(errno));
    }
    myfile2.close();


    kv->thread_local_entry(); //We need this to use thread_local variables 

    const u32 NUM_THREADS = conf->thread_nr_m;

    pthread_t threads[NUM_THREADS];
    pthread_t population_thread_arr[conf->num_population_threads];
    int rc;
    void *status;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    const int cores = sysconf(_SC_NPROCESSORS_ONLN);

    struct timer _timer = timer(conf->frequency);
    // Initialize and set thread joinable
    if (conf->num_population_threads) {
        PRINT_FORMAT("Starting population with BULK_SIZE %u", BULK_SIZE);


        _timer.start_t();
        //Populate
        for (i = 0; i < conf->num_population_threads; i++) {
            rc = pthread_create(&population_thread_arr[i], &attr, populate_loop, population_states[i]);
            if (rc) {
                FATAL("Error: unable to create population thread %d\n", rc);
            }
            cpu_set_t cpuSet;
            CPU_ZERO(&cpuSet);
            CPU_SET(i % cores, &cpuSet);

#ifdef AFFINITY_TH
            rc = pthread_setaffinity_np(population_thread_arr[i], sizeof(cpu_set_t), &cpuSet);
             if (rc) {
                FATAL("Error: could not pin thread %d to core %d\n", i, (i % cores));
            } else {
                TRACE_FORMAT("Pinning thread %d to core %d", i, (i % cores));
            }
#else
            rc = 0;
            TRACE_FORMAT("Not pinning thread %d", i);
#endif


        }


        for (i = 0; i < conf->num_population_threads; i++) {
            rc = pthread_join(population_thread_arr[i], &status);
            if (rc) {
                FATAL("Error: unable to join %d\n", rc);
            }
            fkvb_thread_state *state = population_states[i];
            std::stringstream ss;
            ss << conf->xput_file.c_str() << ".loadxput";
            if (!i)PRINT_FORMAT("Dumping  xput to %s", ss.str().c_str());
            state->dump_xputs(ss.str().c_str());
        }
#if 0
        PRINT_FORMAT(">> Checking number of keys at the end of the population <<");
        if (!conf->instance_id) {
            u64 written;
            while ( (! failed) && (written = kv->get_size()) < conf->num_keys) {
                PRINT_FORMAT("Written keys %lu vs %u", written, conf->num_keys);
                usleep(1000000);
            }

            PRINT_FORMAT("POPULATION ENDED. Written %lu keys in %lu ms", written, _timer.stop_t_milli());
        }
#else
        PRINT_FORMAT(">> SKIPPING KEY_CHECK <<");
        PRINT_FORMAT("POPULATION ENDED in %lu ms", _timer.stop_t_milli());
        kv->print_stats();
#endif


        if (!population_barrier) {
            usleep(5000000);
        } else {
            signal_barrier(conf->load_barrier_file.c_str());
            while (population_barrier) {
                usleep(1000000);
                //PRINT("WAITING FOR SIGUSR2");
            }
            PRINT("OUT OF SIGUSR2");
        }
    } else {
        PRINT_FORMAT("SKIPPING POPULATION");
    }

    populating = false;

    if (NUM_THREADS) {
        PRINT_FORMAT("Running with %d threads", NUM_THREADS);

        _timer.start_t();
        for (i = 0; i < NUM_THREADS; i++) {
            rc = pthread_create(&threads[i], &attr, tx_loop, states[i]);
            if (rc) {
                FATAL("Error: unable to create thread %d\n", i);
            }

            cpu_set_t cpuSet;
            CPU_ZERO(&cpuSet);
            CPU_SET(i % cores, &cpuSet);
#ifdef AFFINITY_TH
            rc = pthread_setaffinity_np(threads[i], sizeof(cpu_set_t), &cpuSet);
            if (rc) {
                FATAL("Error: could not pin thread %d to core %d\n", i, (i % cores));
            } else {
                TRACE_FORMAT("Pinning thread %d to core %d", i, (i % cores));
            }
#else
            rc = 0;
            TRACE_FORMAT("Not pinning thread %d", i)
#endif


        }
        // free attribute and wait for the other threads
        pthread_attr_destroy(&attr);


        for (i = 0; i < NUM_THREADS; i++) {
            rc = pthread_join(threads[i], &status);
            if (rc) {
                FATAL("Error: unable to join %d\n", rc);
            }
            //fprintf(stdout, "Completing thread %d with rc %d\n", i, rc);
            fkvb_thread_state *state = states[i];
            std::stringstream ss;
            ss << conf->xput_file.c_str() << ".runxput";
            state->dump_xputs(ss.str().c_str());

        }
        PRINT_FORMAT("Time taken %lu ms", _timer.stop_t_milli());
#if 0
        PRINT_FORMAT("Checking number of items at the end of the test");
        if (1 && !conf->instance_id) {
            PRINT_FORMAT("Num keys at the end of the test %lu\n", kv->get_size());
        }
#else
        PRINT_FORMAT("*NOT* Checking number of items at the end of the test");
#endif
        PRINT_FORMAT("Shutting down the KV");
    } else {
        PRINT("Only population: skipping client test");
    }

    kv->print_stats();
    kv->thread_local_exit();
    kv->shutdown();

}

seed_t get_random_seed() {

#if defined(_LINUX)
    struct timeval tv;
    struct timezone tz;
    gettimeofday(&tv, &tz);
    return tv.tv_usec;

    /* Changed this for compatibility with PowerPC
       return rdtsc();
    */

#elif defined(_AIX)
    timebasestruct_t tb;
  read_wall_time(&tb, TIMEBASE_SZ);
  time_base_to_time(&tb, TIMEBASE_SZ);
  return tb.tb_low;
#else
    ASSERT(0);
#endif
}

double compute_zipf_skew(int access_pct, int address_pct, unsigned long long n) {

    static double cached_skew = -1;

    if (cached_skew != -1) {
        return cached_skew;
    }
    double percentiles[100];
    int times = 1000000;
    double quantum = (1 / (double) times) * 100;
    int i = 0;
    double s = 1.0;
    int converged = 0;

    TRACE_FORMAT("Target access %% is %d, target accessed keys %% are %d, #keys %llu", access_pct, address_pct, n);

    while (!converged) {
        struct zipf_io_pattern iop(n, 0, s);
        bzero(percentiles, 100 * sizeof(double));

        /* distribute accesses */
        for (i = 0; i < times; i++) {
            int where = iop.next();
            double where_pct = (where / (double) n) * 100;
            percentiles[(int) where_pct] += quantum;
        }

        /* compute cummulative accesses */
        for (i = 1; i < 100; i++)
            percentiles[i] += percentiles[i - 1];

        int cur_access_pct = percentiles[address_pct];

        /* printf("Got %d/%d\n", cur_access_pct, address_pct); */

        if (cur_access_pct == access_pct) {
            converged = 1;
        } else if (cur_access_pct < access_pct) {
            /* need more skew */
            s += 0.01;
        } else {
            /* need less skew */
            s -= 0.01;
        }
        /*TRACE_FORMAT("%d%% of the keys is getting %f%% of accesses. Trying new skew %f", address_pct,
                     percentiles[address_pct], s);*/

    }
    cached_skew = s;
    return s;
}

const char *op_to_string(OPS op) {
    const char *s = 0;
#define VAL(p) case(p): s = #p; break;
    switch (op) {
        VAL(OP_READ);
        VAL(OP_INSERT);
        VAL(OP_RMW);
        VAL(OP_SCAN);
        VAL(OP_UPDATE);
        VAL(OP_GENERIC);
        VAL(OP_INIT);
        VAL(OP_COMMIT);
    case OP_LAST:
    default:
        assert(0);
        break;
    }
    return s;
}


template
class FKVB<int>;


//TODO: Decouple fkvb interface and internals
//FKVB should take only params. Then a factory method should create
//The templated kvs
