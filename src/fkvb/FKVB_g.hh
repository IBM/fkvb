/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef FKVB_G_HH
#define FKVB_G_HH

class FKVB_g {
public:
    virtual void run() = 0;

    virtual void sigusr1_handler(int s) = 0;
    virtual void sigusr2_handler(int s) = 0;
    virtual void sigint_handler(int s) = 0;

};

#endif //FKVB_G_HH
