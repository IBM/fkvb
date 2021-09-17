/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef FKVB_RND_HH
#define FKVB_RND_HH

#include <cstddef>
#include "types.hh"
#include "r_48.hh"
#include <sys/time.h>
#include "zipfian_fkvb.hh"


#define _LINUX 1
//#define RND_MT
typedef unsigned long long int seed_t;

seed_t get_random_seed(void);

struct io_pattern {
    size_t _beg;
    size_t _end;

    io_pattern(size_t e, size_t b = 0) : _beg(b), _end(e) {}

    virtual size_t next() = 0;

    virtual ~io_pattern() {}
};


struct seq_io_pattern : io_pattern {
    size_t _pos;

    seq_io_pattern(size_t e, size_t pos = 0, size_t b = 0)
            : io_pattern(e, b), _pos(pos) {}

    virtual size_t next() {
        size_t rval = _pos++;
        if (_pos >= _end) {
            _pos = _beg;
        }
        return rval;
    }
};

/*
struct const_io_pattern : io_pattern {

    const_io_pattern(size_t e, size_t b = e)
            : io_pattern(e, e), {}

    virtual size_t next() {
        return _beg;
    }
};
 */
#include <random>
//Uncomment to use the std::mt19937_64 random generator
//#define RND_MT

struct rand_io_pattern : io_pattern {
    rand48 _rng;

#ifdef RND_MT
    std::mt19937_64 *generator;
#endif

    rand_io_pattern(size_t e, size_t b = 0, long seed = 0)
            : io_pattern(e, b), _rng(seed ? seed : get_random_seed()) {
#ifdef RND_MT
        generator = new std::mt19937_64(seed == 0 ? get_random_seed() : seed);
#endif
    }

    void reset(long seed) {
#ifdef RND_MT
        generator->seed(seed);
#else
        _rng.seed(seed);
#endif
    }

    virtual size_t next() {
#ifdef RND_MT
        return _beg + (((size_t)(*generator)()) % (_end - _beg));
#else
        return _beg + _rng.randn(_end - _beg);
#endif
    }
};

struct const_io_pattern : io_pattern {

    const_io_pattern(size_t e, size_t b) : io_pattern(e, b) {}

    virtual size_t next() {
        return _beg;
    }
};


struct zipf_io_pattern : io_pattern {
    zipfian _rng;

    zipf_io_pattern(size_t e, size_t b = 0, double skew = 1.0, long seed = 0)
            : io_pattern(e, b), _rng(e - b, skew, seed ? seed : get_random_seed()) {}

    virtual size_t next() {
        return _beg + _rng();
    }
};

struct shifted_zipf_io_pattern : io_pattern {
    zipfian _rng;
    size_t _shift_offset;

    shifted_zipf_io_pattern(size_t e, size_t b = 0, double skew = 1.0,
                            size_t shift_offset = 0, long seed = 0)
            : io_pattern(e, b), _rng(e - b, skew, seed ? seed : get_random_seed()),
              _shift_offset(shift_offset) {}

    virtual size_t next() {
        return (_beg + _rng() + _shift_offset) % (_end - _beg);
    }
};


#endif //FKVB_RND_HH
