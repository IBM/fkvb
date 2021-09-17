/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef R_64_HH
#define R_64_HH

#include <stdint.h>
#include <time.h>


/*
 * Return a 64-bit integer drawn from the uniform distribution on [0, 2^64 - 1]
 *
 * The 64-bit kiss generator pseudo-random number, combining:
 *  - the congruential generator x_n = 6906969069x_{n-1} + 1234567, period 2^64;
 *  - a 3-shift shift-register generator, period 2^64 - 1;
 *  - a 64-bit multiply-with-carry generator, period ?
 *
 * The overall period is approximately 2^250 ~ 10^74.
 * The seeds nx, ny, nz and nc must be set before calling this routine and
 * **must** be... (64 bits each for of nx, ny, nz and 58-bits for nc).
 *
 * See: George Marsaglia <gmarsag...@gmail.com> 64-bit KISS RNGs, Article <> in
 * Usenet newsgroups: sci.math, comp.lang.c, comp.lang.fortran, 28
 * February 2009.
 */

struct kiss_rng64 {

    uint64_t _mx, _my, _mz, _mc;

    kiss_rng64() {
        _mx = timestamp_ns();
        _my = timestamp_ns();
        _mz = timestamp_ns();
        _mc = timestamp_ns();
    }

    kiss_rng64(uint64_t mx, uint64_t my, uint64_t mz, uint64_t mc) {
        _mx = mx;
        _my = my;
        _mz = mz;
        _mc = mc;
    }

    uint64_t timestamp_ns() {
        struct timespec tv;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tv);
        return tv.tv_nsec + tv.tv_sec * 1000000000ULL;
    }

    /* The main function: each invocation will return a pseudo-random 64-bit number */
    uint64_t next() {
        uint64_t t;

        /* Congruential generator */
        _mx = 6906969069ULL * _mx + 1234567;

        /* 3-shift shift-register generator */
        _my ^= (_my << 13);
        _my ^= (_my >> 17);
        _my ^= (_my << 43);

        /* Multiply-with-carry generator */
        t = (_mz << 58) + _mc;
        _mc = (_mz >> 6);
        _mz += t;
        _mc += (_mz < t);

        return (_mx + _my + _mz);
    }
};


#endif //R_64_HH
