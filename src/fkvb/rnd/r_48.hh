/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */
#ifndef __RAND48_H
#define __RAND48_H

#include <stdint.h>

/* In 64-bit land it's really easy to manipulate the bits of an IEEE
   double because the bit ordering is part of the spec and therefore
   free from endianness issues.
*/
struct rand48 {
    enum { DEFAULT_SEED=0x330eabcd1234ull };
    rand48(int64_t s=DEFAULT_SEED) { seed(s); }
    void seed(int64_t s) { state = mask(s); }
    size_t rand() { return update(); }
    int64_t lrand() { return update(); }
    double drand();
    size_t randn(size_t max) { return max * drand(); }

    uint64_t state;
    uint64_t update();
    uint64_t mask(uint64_t x) { return x & 0xffffffffffffull; }
};

inline uint64_t rand48::update() {
    state = mask(state*0x5deece66dull + 0xb);
    return state;
}

inline double rand48::drand() {
    /* In order to avoid the cost of multiple floating point ops we
       conjure up a double directly based on its bitwise
       representation:
       |S|EEEEEEEEEEE|FFFFFF....F|
       (11 bits)   (52 bits)
       where V = (-1)**S * 2**(E-1023) * 1.F (ie F is the fractional
       part of an implied 53-bit fixed-point number which can take
       values ranging from 1.000... to 1.111...).
       The idea is to right-justify our random bits in the mantissa
       field. With that done, setting S=0 and E=0x403=1027 gives
       (-1)**0 * 2**(1027-1023) * 1.0F = 10.F, which is always
       normalized. We then subtract 16.0 to get the answer we actually
       want -- 0.F -- and make the hardware normalize it for us.
    */
    union { int64_t n; double d; } u = {
            (int64_t) ((0x403ull << 52) | update())
    };
    return u.d-16.0;
}

#endif
