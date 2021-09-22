#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Authors: Diego Didona (ddi@zurich.ibm.com)
#
import sys
import traceback


def main(argv):
    if not len(argv) == 3:
        print("Three parameters expected. xput.data output.data tics_per_sec")
        sys.exit(1)
    file_in = argv[0]
    file_out = argv[1]
    tics_per_usec = float(argv[2]) / 1000000

    file = open(file_in)
    # Format is   thread_id time(sec) num_ops (i.e., xput per second)
    xputs = {}
    cumul = {}
    debt = {}
    p50_insert = {}
    p99_insert = {}
    p50_generic = {}
    p99_generic = {}
    avg_init = {}
    avg_commit = {}
    p50_init = {}
    p99_init = {}
    p50_commit = {}
    p99_commit = {}
    avg_update = {}
    p50_update = {}
    p99_update = {}
    p50_generic_begin = {}
    p50_generic_commit = {}
    p50_generic_total = {}
    p99_generic_begin = {}
    p99_generic_commit = {}
    p99_generic_total = {}
    t_count = 0
    for line in file:
        split = line.split()
        t = float(split[0])
        if t + 1 > t_count:  # threads start at 0
            t_count = t + 1
        # convert seconds to int so that then sorting is on N (1 2 3) and not on lexicographic order (1 10 100...)
        s = int(split[1])

        x = split[2]
        l = split[3]
        d = split[4]
        if s not in xputs:
            xputs[s] = float(0)
            cumul[s] = float(0)
            debt[s] = float(0)
            p50_generic[s] = float(0)
            p99_generic[s] = float(0)
            p50_insert[s] = float(0)
            p99_insert[s] = float(0)
            avg_init[s] = float(0)
            p50_init[s] = float(0)
            p99_init[s] = float(0)
            avg_commit[s] = float(0)
            p50_commit[s] = float(0)
            p99_commit[s] = float(0)
            avg_update[s] = float(0)
            p50_update[s] = float(0)
            p99_update[s] = float(0)
            p50_generic_begin[s] = 0.
            p50_generic_commit[s] = 0.
            p50_generic_total[s] = 0.
            p99_generic_begin[s] = 0.
            p99_generic_commit[s] = 0.
            p99_generic_total[s] = 0.

        xputs[s] = xputs[s] + float(x)
        cumul[s] = cumul[s] + float(l)
        debt[s] = debt[s] + float(d)
        p50_insert[s] = p50_insert[s] + float(split[5])
        p99_insert[s] = p99_insert[s] + float(split[6])
        p50_generic[s] = p50_generic[s] + float(split[7])
        p99_generic[s] = p99_generic[s] + float(split[8])

        avg_init[s] = avg_init[s] + float(split[9])
        p50_init[s] = p50_init[s] + float(split[10])
        p99_init[s] = p99_init[s] + float(split[11])
        avg_commit[s] = avg_commit[s] + float(split[12])
        p50_commit[s] = p50_commit[s] + float(split[13])
        p99_commit[s] = p99_commit[s] + float(split[14])
        if len(split) > 14:
            avg_update[s] = avg_update[s] + float(split[15])
            p50_update[s] = p50_update[s] + float(split[16])
            p99_update[s] = p99_update[s] + float(split[17])
        if len(split) > 18:
            p50_generic_begin[s] = p50_generic_begin[s] + float(split[18])
            p50_generic_commit[s] = p50_generic_commit[s] + float(split[19])
            p50_generic_total[s] = p50_generic_total[s] + float(split[20])
            p99_generic_begin[s] = p99_generic_begin[s] + float(split[21])
            p99_generic_commit[s] = p99_generic_commit[s] + float(split[22])
            p99_generic_total[s] = p99_generic_total[s] + float(split[23])
    file.close()

    file = open(file_out, "w")
    file.write("#Second Xput avg_generic p50_insert p99_insert p50_generic p99_generic "
               "avg_init p50_init p99_init avg_commit p50_commit p99_commit avg_update p50_update p99_update "
               "p50_generic_b p50_generic_c p50_generic_total "
               "p99_generic_b p99_generic_c p99_generic_total\n")
    for s in sorted(xputs):
        avg = float((cumul[s] / tics_per_usec) / xputs[s]) if xputs[s] > 0 else 0
        i50 = (p50_insert[s] / tics_per_usec) / t_count
        i99 = (p99_insert[s] / tics_per_usec) / t_count
        g50 = (p50_generic[s] / tics_per_usec) / t_count
        g99 = (p99_generic[s] / tics_per_usec) / t_count
        initavg = (avg_init[s] / tics_per_usec) / t_count
        init50 = (p50_init[s] / tics_per_usec) / t_count
        init99 = (p99_init[s] / tics_per_usec) / t_count
        commitavg = (avg_commit[s] / tics_per_usec) / t_count
        commit50 = (p50_commit[s] / tics_per_usec) / t_count
        commit99 = (p99_commit[s] / tics_per_usec) / t_count
        if len(avg_update) >= s:  # Only if we have actually collected that stat
            updateavg = (avg_update[s] / tics_per_usec) / t_count
            update50 = (p50_update[s] / tics_per_usec) / t_count
            update99 = (p99_update[s] / tics_per_usec) / t_count
        else:
            updateavg = 0
            update50 = 0
            update99 = 0
        if len(p50_generic_begin) >= s:
            bg50 = (p50_generic_begin[s] / tics_per_usec) / t_count
            cg50 = (p50_generic_commit[s] / tics_per_usec) / t_count
            tg50 = (p50_generic_total[s] / tics_per_usec) / t_count
            bg99 = (p99_generic_begin[s] / tics_per_usec) / t_count
            cg99 = (p99_generic_commit[s] / tics_per_usec) / t_count
            tg99 = (p99_generic_total[s] / tics_per_usec) / t_count
        else:
            bg50 = 0
            cg50 = 0
            tg50 = 0
            bg99 = 0
            cg99 = 0
            tg99 = 0

        file.write(
            "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10} {11} {12} {13} {14} {15} {16} {17} {18} {19} {20} {21}\n".format(
                s, xputs[s], avg,
                i50, i99, g50, g99,
                initavg, init50, init99,
                commitavg, commit50, commit99,
                updateavg, update50, update99,
                bg50, cg50, tg50, bg99, cg99, tg99))
    file.flush()


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except:
        print("An error occurred.")
        traceback.print_exc()
        sys.exit(1)
