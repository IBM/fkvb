#!/bin/bash
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Authors: Diego Didona (ddi@zurich.ibm.com)
#

EXEC="bin/fkvb"
CLUSTER_FILE="/home/ddi/fdb.flex13"
NUM_KEYS="10000"
KEY_SIZE=31
VALUE_SIZE=600
LB="lib/fdb/620"


NR_LOADING_CLIENTS=2
NR_LOADING_THREADS=2

NR_CLIENTS=2
NR_THREADS=2

THINK_TIME=0
OPS_PER_TX=1
RO_PERC=100
GRV_CACHE_MS=0
TEST_SEC=60
#Nominal frequency in Hz of the CPU. It assumes all CPUs run at the same frequency and that any automatic scaling is disabled. E.g.,  a CPU with 2.9 GHz will have 2900000000 as value
FREQ="2900000000"  

OUT_DIR="/mnt/ddi/tmp"

SEED="12334554321"

function usage(){
	echo "Usage: ./workload [LOAD | RUN]"
}

if [[ $# -ne 1 ]]; then
	usage
	exit 1
fi

if [[ $1 == "LOAD" ]]; then
	for c in $(seq 0 $(( ${NR_LOADING_CLIENTS} -1 )));do
		echo "Spawning $c"
		LD_LIBRARY_PATH=${LB}:${LB_LIBRARY_PATH} ${EXEC} -f "DUMMY" --xput ${OUT_DIR}/${c}.load.xput.out  --freq ${FREQ} --seed ${SEED} -u 13 --t_population ${NR_LOADING_THREADS} -t 0 --num_keys ${NUM_KEYS} --key_size ${KEY_SIZE} --value_size const${VALUE_SIZE} --key_type random --config_file ${CLUSTER_FILE} --id ${c} --num_clients ${NR_LOADING_CLIENTS} 2>${OUT_DIR}/${c}.load.err | tee ${OUT_DIR}/${c}.load.out  &
	done
	echo "Clients spawned. Now waiting for them to end"
	wait
elif [[ $1 == "RUN" ]]; then
	for c in $(seq 0 $(( ${NR_CLIENTS} - 1 )));do
		LD_LIBRARY_PATH=${LB}:${LD_LIBRARY_PATH} ${EXEC} -f "DUMMY" --xput ${OUT_DIR}/${c}.run.xput.out  --freq ${FREQ} --seed ${SEED} -u 13 --t_population 0 -t ${NR_THREADS} --num_keys ${NUM_KEYS} --key_size ${KEY_SIZE} --value_size const${VALUE_SIZE} --key_type random --config_file ${CLUSTER_FILE} --id ${c} --dap uniform --read_perc 0 --update_perc 0 --generic_perc 100 --generic_rp ${RO_PERCENTAGE} --generic_ops ${OPS_PER_TX} --grv_cache_ms ${GRV_CACHE_MS} --dur sec${TEST_SEC} --key_type random  --sleep_time_us ${THINK_TIME}  2>${OUT_DIR}/${c}.run.err | tee ${OUT_DIR}/${c}.run.out &
	done
	echo "Clients spawned. Now waiting for them to end"
	wait
else
	usage
	exit 1
fi
