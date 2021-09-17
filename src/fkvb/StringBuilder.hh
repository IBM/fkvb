/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef STRINGBUILDER_HH
#define STRINGBUILDER_HH

#include "rnd/fkvb_rnd.hh"
#include "defs.hh"
#include <cassert>
#include <string.h>

#define PREFIX "U: "


//TODO: Random strings
/*
 * 1. Have a seed on the key generator
 * 2. Re-Init the generator with f(seed, key_id)
 * 3. Generate the length of the string key
 *
 * If we want to keep the size of the values w/o reading first we can do the same thing
 * 1. Have a seed on the value generator
 * 2. Re-init the generator with f(seed, key_id)
 * 3. Generate the length and content of the value
 *
 * Random values are preferable because they are harder to compress
 */

#define rnd_buffer_size  (1 << 20)


struct string_builder {


    io_pattern *_pattern;

    string_builder(io_pattern *pattern) : _pattern(pattern) {
        init_random_buffer();
    };

    size_t next_size() {
        return _pattern->next();
    }

private:

    void init_random_buffer() {
        rand_io_pattern io = rand_io_pattern(127, 65);

        unsigned int i;
        for (i = 0; i < rnd_buffer_size; i++) {
            rnd_buffer[i] = (char) io.next();
        }
    }


protected:
    char rnd_buffer[rnd_buffer_size];


    static size_t digits(size_t x) {
        return (x < 10 ? 1 :
                (x < 100 ? 2 :
                 (x < 1000 ? 3 :
                  (x < 10000 ? 4 :
                   (x < 100000 ? 5 :
                    (x < 1000000 ? 6 :
                     (x < 10000000 ? 7 :
                      (x < 100000000 ? 8 :
                       (x < 1000000000 ? 9 :
                        10)))))))));
    }
};


struct value_string_builder_rnd : string_builder {

    unsigned int index = 0;

    value_string_builder_rnd(io_pattern *pattern) : string_builder(pattern) {
    };


    void build(char *out_buf, size_t *out_size) {

        *out_size = next_size();
        assert(*out_size < rnd_buffer_size);
        if (index + (*out_size) >= rnd_buffer_size) {
            index = 0; //KISS. Avoid wrap-around
        }
        memcpy((void *) out_buf, &rnd_buffer[index], *out_size);
        index += (*out_size);
    }

    const char *_build(size_t *out_size) {

        *out_size = next_size();
        assert(*out_size < rnd_buffer_size);
        if (index + (*out_size) > rnd_buffer_size) {
            index = 0; //KISS. Avoid wrap-around
        }
        const char *ret = &rnd_buffer[index];
        index += (*out_size);
        return ret;
    }
};

struct key_string_builder : string_builder {

    key_string_builder(io_pattern *pattern) : string_builder(pattern) {};

    void _fdb_build(const int key, char *out_buf, size_t *out_size) {
        *out_size = 16;
        sprintf(out_buf, "%0*d", (int) *out_size, key);
        TRACE_FORMAT("Next key %s", out_buf);
    }


    void build(const int key, char *out_buf, size_t *out_size) {
        *out_size = (sprintf(out_buf, "%s%d", PREFIX, key)); //Add +1 to consider the final null byte
        TRACE_FORMAT("Next key %s", out_buf);
    }
};


struct prefix_key_string_builder : string_builder {

    const size_t key_size;
    const size_t payload_size;
    const std::string prefix;
    const unsigned int prefix_len;

    prefix_key_string_builder(io_pattern *pattern, size_t _key_size, size_t num_keys) :
            string_builder(pattern), key_size(_key_size), payload_size(key_size - sizeof(PREFIX) + 1), prefix(std::string(PREFIX)),
            prefix_len(strlen(PREFIX)) {
        if (payload_size < digits(num_keys) || key_size <= strlen(PREFIX)) {

            FATAL("Cannot support %zu keys with a max key size of %zu given the prefix %s", num_keys, key_size, PREFIX);
        }
        TRACE_FORMAT("Size of prefix %s is %zu; key size is %zu; payload size is %zu", prefix.c_str(),
                     prefix_len, key_size, payload_size);
    };

    prefix_key_string_builder(io_pattern *pattern, size_t _key_size, size_t num_keys, std::string &pref) :
            string_builder(pattern), key_size(_key_size), payload_size(key_size - pref.length()),
            prefix(pref),
            prefix_len(prefix.length()) {
        if (payload_size < digits(num_keys) || key_size <= prefix.length()) {

            FATAL("Cannot support %zu keys with a max key size of %zu given the prefix %s", num_keys, key_size, PREFIX);
        }
        TRACE_FORMAT("Size of prefix %s is %zu; key size is %zu; payload size is %zu", prefix.c_str(),
                     prefix_len, key_size, payload_size);
    };


    virtual void build(const int key, char *out_buf, size_t *out_size) {

        char *ptr = out_buf;
        unsigned int i;
        const unsigned int dig = digits(key);
        const unsigned int zeroes = payload_size - dig;
        const char *pr = prefix.c_str();
        //memset(out_buf + sizeof(PREFIX) - 1, 0, KEY_SIZE);
        //fprintf(stdout, "prefix = %.*s. Len %d\n", prefix_len, pr, prefix_len);
        //For now, we copy the prefix every time
        //In the future, the buffer will already store the prefix and we'll just have to fill the suffix
        for (i = 0; i < prefix_len; i++) {
            *ptr = pr[i];
            ptr++;
        }
        //fprintf(stdout, "prefix copied\n");fflush(stdout);

        //memset(ptr, 48, zeroes);
        //ptr += zeroes;

        for (i = 0; i < zeroes; i++) {
            *ptr = '0';
            ptr++;
        }
        //fprintf(stdout, "%d zeroes copied for key %d of size %zu\n",zeroes,key,key_size);fflush(stdout);
        int k = key, d;
        ptr = out_buf + key_size - 1;
        //Note that if the string is null initialized, there is a null temrination in the middle of the string
        //So only the last print will show the whole key
        //fprintf(stdout,"Copying %d digits\n",dig);
        for (i = dig; i > 0; i--) {
            d = k % 10;
            //fprintf(stdout,"copying %d %d\n",i,d);
            //48 is ascii for '0'. So we sum 48 to the digit int value to get its ascii representation
            *ptr = (char) (d + 48);
            k = (k - d) / 10;
            ptr--;
        }
        //fprintf(stdout,"Copied %d digits\n",dig);
        *out_size = key_size;
        TRACE_FORMAT("Next key %.*s", (int) key_size, out_buf);
    }

private :
    void _build(const int key, char *out_buf, size_t *out_size) {
        (sprintf(out_buf, "%s%0*d", prefix.c_str(), (int) payload_size, key));
        *out_size = key_size;
        TRACE_FORMAT("Next key %s", out_buf);
    }
};

struct prefix_key_string_builder_rnd : prefix_key_string_builder {

    using prefix_key_string_builder::prefix_key_string_builder;

    void build(const int key, char *out_buf, size_t *out_size) {
        static unsigned long k_s = 1118388721419649241;
        //Reset deterministically the seed of the rnd pattern so that each time we ask for a given key index, we get the same chars
        ((rand_io_pattern *) _pattern)->_rng.seed(k_s + key);
        char *ptr = out_buf;
        unsigned int i;
        const char *pr = prefix.c_str();
        //Copy the prefix at the beginning of the key
        //For now, we copy the prefix every time
        //In the future, the buffer will already store the prefix and we'll just have to fill the suffix
        for (i = 0; i < prefix_len; i++) {
            *ptr = pr[i];
            ptr++;
        }
        //Suffix of the key is random
        for (i = 0; i < (key_size - prefix_len); i++) {
            *ptr = (char) _pattern->next();
            ptr++;
        }
        *out_size = key_size;
        TRACE_FORMAT("Next key %s", out_buf);
    }

};

//ALL random. No guarantees there's not going to be any overlap
struct key_string_builder_rnd : prefix_key_string_builder {

    key_string_builder_rnd(io_pattern *pattern, size_t _key_size, size_t num_keys) : prefix_key_string_builder(pattern,
                                                                                                               _key_size,
                                                                                                               num_keys) {
        assert(pattern != nullptr);
        _pattern->_beg = 48; //zero char
        _pattern->_end = 48 + 26 + 26 + 10;
    };

    void build(const int key, char *out_buf, size_t *out_size) {
        static unsigned long k_s = 1118388721419649241;
        //Reset deterministically the seed of the rnd pattern so that each time we ask for a given key index, we get the same chars
        //((rand_io_pattern *) _pattern)->_rng.seed(k_s + key);
        ((rand_io_pattern *) _pattern)->reset(k_s + key);
        u32 i;
        for (i = 0; i < key_size; i++) {
            out_buf[i] = (char) _pattern->next();
        }
        *out_size = key_size;
        TRACE_FORMAT("Next key %s", out_buf);
    }
};

#endif //STRINGBUILDER_HH
