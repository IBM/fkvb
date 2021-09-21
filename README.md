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

## License

This project is licensed under the Apache License 2.0.
If you would like to see the detailed LICENSE click [here](LICENSE).

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.
Note that this repository has been configured with the [DCO bot](https://github.com/probot/dco).
