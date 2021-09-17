/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */


#include "kv-ordered.hh"
#include "types.hh"
#include "debug.hh"
#include <inttypes.h>
#include "fkvb_test_conf.hh"
#include "FKVB_g.hh"
#include "fkvb_factory.hh"

struct fkvb_test_conf conf;
FKVB_g *fkvb;

#include <typeinfo>
#include <time.h>
#include <signal.h>

void sigusr1_handler(int s);

void sigusr2_handler(int s);

void sigint_handler(int s);



void sigint_handler(int s) {
    PRINT_FORMAT("CAUGHT SIGINT");
    fkvb->sigint_handler(s);
}
void sigusr1_handler(int s) {
    PRINT_FORMAT("CAUGHT SIGUSR1");
    fkvb->sigusr1_handler(s);
}

void sigusr2_handler(int s) {
    PRINT_FORMAT("CAUGHT SIGUSR2");
    fkvb->sigusr2_handler(s);
}

int main(const int argc, char *argv[]) {

    struct sigaction h;

    h.sa_handler = sigusr1_handler;
    sigemptyset(&h.sa_mask);
    h.sa_flags = 0;
    sigaction(SIGUSR1, &h, NULL);

    struct sigaction h2;

    h2.sa_handler = sigusr2_handler;
    sigemptyset(&h2.sa_mask);
    h2.sa_flags = 0;
    sigaction(SIGUSR2, &h2, NULL);


    struct sigaction h3;
    h3.sa_handler = sigint_handler;
    sigemptyset(&h3.sa_mask);
    h3.sa_flags = 0;
    sigaction(SIGINT, &h3, NULL);

    int p;
    for(p=0;p<argc;p++)fprintf(stdout,"%s, ",argv[p]);
    fprintf(stdout,"\n");fflush(stdout);

    int rc;


    struct ParseArgs parsedArgs(argc, argv);
    rc = conf.parse_args(parsedArgs);

    if (0 != rc || conf.help_m) {
        conf.print_usage(argv[0]);
        if (!conf.help_m)
            FATAL("Error %s (%d).", strerror(rc), rc);
        return -rc;
    } else if (parsedArgs.has_unused_args()) {
        parsedArgs.print_unused_args(std::cerr);
        conf.print_usage(argv[0]);
        return EINVAL;
    }

    //KV -specific params should be sanitized by the KV
    conf.validate_and_sanitize_parameters();
    fkvb_factory *yf = new fkvb_factory();
    fkvb = yf->build_fkvb(&conf);
    if (nullptr == fkvb) {
        conf.print_usage(argv[0]);
    } else {
        fkvb->run();
    }
    return 0;
}






