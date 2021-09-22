#!/bin/bash
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Authors: Diego Didona (ddi@zurich.ibm.com)
#
RESULTS="/Users/ddi/repo/fkvb-public"  #Folder where the runxput files have been stored by fkvb
FREQ="2900000000"  #Nominal frequency of the CPU in Hz
MIN_ID=40  #Lowest id corresponding to output
MAX_ID=40  #Max id corresponding to output

for ID in $(seq $MIN_ID $MAX_ID); do
        xput="${RESULTS}/${ID}.xput"
        XPUT_FILE_RUN="${RESULTS}/${ID}.runxput"
        python3 xput-process.py "${xput}.runxput" ${XPUT_FILE_RUN} ${FREQ} > ${RESULTS}/$ID.errout 3>&1 &
done
