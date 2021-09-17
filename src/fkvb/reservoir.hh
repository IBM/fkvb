/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef RESERVOIR_HH
#define RESERVOIR_HH

struct reservoir {

    //The number of samples determines the max granularity of your percentile
    //Should we add a warning that if you request a granularity 10e^-x you must have at least 10e^x samples?
#define RESERVOIR_SIZE 1000
    unsigned long values[RESERVOIR_SIZE];
    unsigned long curr_sorted_values[RESERVOIR_SIZE];
    unsigned long curr_sorted_samples;
    bool sorted;
    unsigned int num_samples;
    struct rand_io_pattern rnd;


    ~reservoir() {
    }

    reservoir() : sorted(false), num_samples(0), rnd(rand_io_pattern((size_t) RESERVOIR_SIZE, (size_t) 0, get_random_seed())) {
    }

    reservoir(long seed) : sorted(false), num_samples(0), rnd(rand_io_pattern((size_t) RESERVOIR_SIZE, (size_t) 0, seed)) {
    }

    void reset() {
        sorted = false;
        num_samples = 0;
        rnd._end = RESERVOIR_SIZE;
        curr_sorted_samples = 0;
    }

    void add(unsigned long value) {
        //printf("Adding %lu. num samples (%p) is %u. Values %p\n",value,&num_samples,num_samples,values);
        if (num_samples < RESERVOIR_SIZE) {
            values[num_samples++] = value;
        } else {
            num_samples++;
            rnd._end = num_samples;
            const int index = rnd.next();
            if (index < RESERVOIR_SIZE) {
                values[index] = value;
            }
        }
    }

    void sort() {
        curr_sorted_samples = num_samples > RESERVOIR_SIZE ? RESERVOIR_SIZE : num_samples;
        std::copy(values, values + curr_sorted_samples, curr_sorted_values);
        std::sort(curr_sorted_values, curr_sorted_values + curr_sorted_samples);
        sorted = true;
    }

    unsigned int num_sorted_samples() {
        if (!sorted) {
            FATAL("Asking percentile on an unsorted array");
        }
        return curr_sorted_samples;
    }

    unsigned long get_avg() {
        if (curr_sorted_samples == 0) {
            return 0;
        }
        if (!sorted) {
            FATAL("Asking percentile on an unsorted array");
        }
        unsigned i = 0;
        unsigned long avg = 0;
        for (; i < curr_sorted_samples; i++) {
            avg += curr_sorted_values[i];
        }
        return avg / curr_sorted_samples;
    }

    unsigned long get_percentile(double perc) {

        if (curr_sorted_samples == 0) {
            return 0;
        }
        if(perc <=0 || perc > 1){
            FATAL("Wrong percentile %f", perc);
        }
        if (!sorted) {
            FATAL("Asking percentile on an unsorted array");
        }
        unsigned int index = (unsigned int) ((perc) * (((double) curr_sorted_samples)));
        //printf("Asking perc %f. curr_sorted samples %lu. Is sorted %d. %p. Index %u, Ret %lu\n",perc,curr_sorted_samples,sorted,&curr_sorted_samples,index,curr_sorted_values[index]);
        return curr_sorted_values[index];
    }
};

#endif //RESERVOIR_HH


struct perc_s {
    unsigned long total, start, body, commit;

    perc_s(unsigned long t, unsigned long s, unsigned long b, unsigned long c) : total(t), start(s), body(b),
                                                                                 commit(c) {}

    perc_s(): total(0),start(0),body(0),commit(0){

    }

    bool operator<(const perc_s &other) const{
        return (total < other.total);
    }

    perc_s &operator+=(const perc_s &other) {
        this->total += other.total;
        this->start += other.start;
        this->body += other.body;
        this->commit += other.commit;
        return *this;
    }

    perc_s operator/(unsigned long num) {
        return perc_s(this->total/num,this->start/num,this->body/num,this->commit/num);
    }
};

template<typename S>
struct t_reservoir {
#define RESERVOIR_SIZE 1000
    S values[RESERVOIR_SIZE];
    S curr_sorted_values[RESERVOIR_SIZE];
    unsigned long curr_sorted_samples;
    bool sorted;
    unsigned int num_samples;
    struct rand_io_pattern rnd;


    ~t_reservoir() {
    }

    t_reservoir() : sorted(false), num_samples(0), rnd(rand_io_pattern((size_t) RESERVOIR_SIZE, (size_t) 0, get_random_seed())) {
    }

    t_reservoir(long seed) : sorted(false), num_samples(0) ,rnd(rand_io_pattern((size_t) RESERVOIR_SIZE, (size_t) 0, seed)) {
    }

    void reset() {
        sorted = false;
        num_samples = 0;
        rnd._end = RESERVOIR_SIZE;
        curr_sorted_samples = 0;
    }

    void add(S &&value) {
        //printf("Adding %lu. num samples (%p) is %u. Values %p\n",value,&num_samples,num_samples,values);
        if (num_samples < RESERVOIR_SIZE) {
            values[num_samples++] = value;
        } else {
            num_samples++;
            rnd._end = num_samples;
            const int index = rnd.next();
            if (index < RESERVOIR_SIZE) {
                values[index] = value;
            }
        }
    }

    void sort() {
        curr_sorted_samples = num_samples > RESERVOIR_SIZE ? RESERVOIR_SIZE : num_samples;
        std::copy(values, values + curr_sorted_samples, curr_sorted_values);
        std::sort(curr_sorted_values, curr_sorted_values + curr_sorted_samples);
        sorted = true;
    }

    unsigned int num_sorted_samples() {
        if (!sorted) {
            FATAL("Asking percentile on an unsorted array");
        }
        return curr_sorted_samples;
    }

    perc_s get_avg() {
        if (curr_sorted_samples == 0) {
            return perc_s();
        }
        if (!sorted) {
            FATAL("Asking percentile on an unsorted array");
        }
        unsigned i = 0;
        S avg;
        for (; i < curr_sorted_samples; i++) {
            avg += curr_sorted_values[i];
        }
        return avg / curr_sorted_samples;
    }

    perc_s get_percentile(double perc) {

        if (curr_sorted_samples == 0) {
            return perc_s();
        }
        if(perc <=0 || perc > 1){
            FATAL("Wrong percentile %f", perc);
        }
        if (!sorted) {
            FATAL("Asking percentile on an unsorted array");
        }
        unsigned int index = (unsigned int) ((perc) * (((double) curr_sorted_samples)));
        //printf("Asking perc %f. curr_sorted samples %lu. Is sorted %d. %p. Index %u, Ret %lu\n",perc,curr_sorted_samples,sorted,&curr_sorted_samples,index,curr_sorted_values[index]);
        return curr_sorted_values[index];
    }
};
