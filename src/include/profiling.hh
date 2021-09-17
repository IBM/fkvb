/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef PROFILING_HH_
#define PROFILING_HH_

#include <ticks.hh>

#if defined(CONF_SDT)

#include <sys/sdt.h>
# if 0
#define PROBE_TICKS_START(id)                                           \
	static thread_local uint32_t __count_ ## id = 0;                    \
	uint64_t __ticks_ ## id ## __start__ = 0;                           \
	bool __sample ## id = (__count_ ## id)++ % 512 == 0;                \
	if (__sample ## id)                                                 \
		__ticks_ ## id ## __start__ = ticks::get_ticks();               \

#define PROBE_TICKS_END(id)  \
do {                                                                      \
	if (__sample ## id) {                                                 \
		uint64_t t = ticks::get_ticks() - (__ticks_ ## id ## __start__);  \
		DTRACE_PROBE1(udepot, id, t);                                     \
	}                                                                     \
} while (0)
#else
#define PROBE_TICKS_START(id) \
	uint64_t __ticks_ ## id ## __start__ = ticks::get_ticks();

#define PROBE_TICKS_END(id)  \
do { \
	uint64_t t = ticks::get_ticks() - (__ticks_ ## id ## __start__);  \
	DTRACE_PROBE1(udepot, id, t);                                     \
} while (0)
#endif

#else // 

#define DTRACE_PROBE(prov,probe)                                          do {} while (0)
#define DTRACE_PROBE1(prov,probe,p1)                                      do {} while (0)
#define DTRACE_PROBE2(prov,probe,p1,p2)                                   do {} while (0)
#define DTRACE_PROBE3(prov,probe,p1,p2,p3)                                do {} while (0)
#define DTRACE_PROBE4(prov,probe,p1,p2,p3,p4)                             do {} while (0)
#define DTRACE_PROBE5(prov,probe,p1,p2,p3,p4,p5)                          do {} while (0)
#define DTRACE_PROBE6(prov,probe,p1,p2,p3,p4,p5,p6)                       do {} while (0)
#define DTRACE_PROBE7(prov,probe,p1,p2,p3,p4,p5,p6,p7)                    do {} while (0)
#define DTRACE_PROBE8(prov,probe,p1,p2,p3,p4,p5,p6,p7,p8)                 do {} while (0)
#define DTRACE_PROBE9(prov,probe,p1,p2,p3,p4,p5,p6,p7,p8,p9)              do {} while (0)
#define DTRACE_PROBE10(prov,probe,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10)         do {} while (0)
#define DTRACE_PROBE11(prov,probe,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11)     do {} while (0)
#define DTRACE_PROBE12(prov,probe,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12) do {} while (0)


#define PROBE_TICKS_START(id) do {} while (0)
#define PROBE_TICKS_END(id)   do {} while (0)

#endif 


#endif 
