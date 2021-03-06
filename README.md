# The Flexible Key-Value Store Benchmarking tool

[![Build Status](https://travis-ci.com/IBM/fkvb.svg?branch=main)](https://travis-ci.com/IBM/fkvb)

FKVB is a benchmarking tool for key-value stores. Right now it only supports FoundationDB (www.foundationdb.org)
# Installation
```
$ make
```

# Running a workload
To run a workload, first spawn an FDB cluster and create the databse through the fdbcli. Then, use FKVB to load it. Finally, run FKVB to run the desired workload.

## Loading an FDB cluster
```
$ ./workload LOAD
````
The important parameters are
* CLUSTER_FILE that has to point to the FDB cluster file used to spawn the cluster
* NR_LOADING_CLIENTS/THREADS that are the  number of client processes and threads per process used to load the DB
* NUM_KEYS that is the number of keys to insert in the DB
* KEY/VALUE_SIZE that are the (fixed) size of keys and of the corresponding values
* OUT_DIR that is a directory where some output files are written
* ID that is the id of each client process
* SEED that is the random seed used by the clients
* FREQ that is the nominal frequency in Hz of the CPU
* CLNT_KNOBS is the set of knobs passed to the FDB client (as of now, only batching parameters)

If running the loading phase on multiple machines, take care that the --num_clients parameter is the total amount of client processes, and that the ids are progressive from 0 to num_clients-1

## Running a workload
```
$ ./workload.sh RUN
```
Important parameters are

* CLUSTER_FILE, NUM_KEYS, KEY/VALUE_SIZE, SEED, which _must_ to be the same as in the LOAD phase
* OUT_DIR where some logs are stored
* OPS_PER_TX that is the number of operation per transaction
* RO_PERC that is the percentage of operations within a transaction that are going to be read operation _on average_. Note that this means that if this value is 0, all operations are going to be write. If it is 100, all operations are going to be read. If it is in (0, 100), then the expected read:write ratio is going to be RO_PERC:100-RO_PERC but it can happen that individual transactions have a different ratio (and can be read-only or write-only)
* GRV_CACHE_MS that is the value (in ms) for caching the GRV before requesting a new one. If  it is 0, a new GRV is asked for each new transaction
* TEST_SEC that is the duration in seconds of the test
* NR_CLIENTS/THREADS that is the number of clients/threads per client that are spawned
* THINK_TIME that is the time in useconds that a thread waits after compelting a transaction before starting a new one
* FREQ that is the nominal frequency in Hz of the CPU
* CLNT_KNOBS is the set of knobs passed to the FDB client (as of now, only batching parameters)

## Post-processing the results
Once a RUN test has finished, each process will generate a file called ID.xput.runxput that contains statistics (throughput and latency) for each thread in the process, at a one-second granularity.
The `process.sh` script can be used to produce an aggregate set of statistics for each process. This scripts invokes the `xput-process` script, that averages the statistics of each thread in a process, and produces a file ID.runxput with such averaged statistics, at a one-second granularity.
Important parameters for `process.sh` are

* RESULTS, that has to point to the same result directory supplied to the `workload RUN` invocation
* MIN/MAX_ID, that are the min and max ids of the processes for which one wants the stats to be produced. For each id, a different file is produced
* FREQ, that is the nominal frequency in Hz of the CPU

For the supplied `workload.sh` file, the following statistics are of interest (all latencies are in microseconds):
* Second: the time corresponding to the subsequent statistics
* Xput: the throughput, in transactions per second
* avg/p50/p99_generic: the average/median/99-th percentile latency of operations. As of now, this statistic assumes `generic_perc 100` in workload.sh
* avg/p50/p99_init/commit: the average/median/99-th percentile latency of init/commit operations.

## License

This project is licensed under the Apache License 2.0.
If you would like to see the detailed LICENSE click [here](LICENSE).

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.
Note that this repository has been configured with the [DCO bot](https://github.com/probot/dco).
