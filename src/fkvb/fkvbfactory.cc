/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#include "fkvb_factory.hh"


fkvb_factory::fkvb_factory() {}

FKVB_g *fkvb_factory::build_fkvb(fkvb_test_conf *conf) {
    switch (conf->type_m) {
        case KV_conf::KV_FDB: {
            KVOrderedFDB<int> *FDB = new KVOrderedFDB<int>(conf);
            if (FDB->init()) {
                ERROR("Error while initing FDB");
                return nullptr;
            }
            PRINT("Initing FDB");
            return new FKVB<int>(FDB, conf);
        }

        case KV_conf::KV_DUMMY: {
            DummyKVOrdered<int> *D = new DummyKVOrdered<int>(conf);
            if (D->init()) {
                ERROR("Error while initing Dummy");
                return nullptr;
            }
            PRINT_FORMAT("Initing %s", KV_conf::type_to_string(conf->type_m).c_str());
            return new FKVB<int>(D, conf);
        }

        default: {
            FATAL("Invalid KV type %d.", conf->type_m);
            return nullptr;
        };
    }
}
