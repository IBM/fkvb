/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef FKVB_FACTORY_HH
#define FKVB_FACTORY_HH

#include "kv.hh"
#include "debug.hh"
#include "fkvb_test_conf.hh"
#include "FKVB_g.hh"
#include "FKVB.hh"
#include "kv-ordered.hh"
#include "defs.hh"
#include "backends.hh"


class fkvb_factory {


public:
    fkvb_factory();

    FKVB_g *build_fkvb(fkvb_test_conf *conf);
};
//};


#endif //FKVB_FACTORY_HH
