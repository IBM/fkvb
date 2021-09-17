/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */


#ifndef ZIPFIAN_FKVB_HH
#define ZIPFIAN_FKVB_HH
#include <cassert>


#include <cmath>
#include <stdio.h>
#include <stdlib.h>
#include "r_64.hh"
#include "r_48.hh"

enum {
    REGION_SIZE = 1
}; // in pages

struct zipfian {
    rand48 _rng;
    kiss_rng64 *_rng64;
    size_t _max_id;
    double _exponent;
    double _cutoff;
    double _mk_inv;
    double _1mcutoff;

    zipfian(size_t n, double s, long seed_val = rand48::DEFAULT_SEED)
            : _rng(seed_val), _max_id(n / REGION_SIZE), _exponent(s) {

        double i_temp_expo = log10((double) _max_id) - 1;
        double coef = (double) 0.16;
        if (i_temp_expo < 0)
            i_temp_expo = 0;

        double dtmp = (1.0 + coef / pow(2.0, i_temp_expo)) * _exponent - 1;
        _mk_inv = -1.0 / dtmp;
        _cutoff = pow(_max_id, -dtmp);
        _1mcutoff = 1 - _cutoff;

        _rng64 = new kiss_rng64(_rng.randn(1UL << 60), _rng.randn(1UL << 60), _rng.randn(1UL << 60),
                                _rng.randn(1UL << 60)); //@ddi
        assert(sizeof(uint64_t) == sizeof(size_t));
    }

    void seed(int64_t s) { _rng.seed(s); }

    size_t operator()() { return next(); }

    size_t next() {
        size_t z = 0;
        double u = _rng.drand();
        u *= _1mcutoff;
        u += _cutoff;
        z = (size_t) pow(u, _mk_inv) - 1;
        if (z >= _max_id)
            z = _max_id - 1;

        size_t offset = _rng64->next() % REGION_SIZE;
        return ((z * REGION_SIZE) + offset);
    }
};


#endif //ZIPFIAN_FKVB_HH
