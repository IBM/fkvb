/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#include "KVOrderedFDB.hh"

#define MAX_RETRY 10

//Maybe if I explicitly set some configs with the API, I cannot log by simply using
//FDB_NETWORK_OPTION_TRACE_ENABLE
//So let's keep this on
//#define LOG_FDB
//#define LOG_TX

static thread_local FDBFuture **generic_futures;
#if defined(LOG_FDB) and defined(LOG_TX)
thread_local u64 tx_id = 0;
thread_local char tx_sid[20];
#endif
extern thread_local u32 tid; //for debugging purposes only

extern thread_local uint64_t zrl_fkvb_begin_latency, zrl_fkvb_commit_latency,  last_grv_wallclock;
extern u64 grv_cache_tics_ms;
thread_local int64_t last_grv;
thread_local u64 last_grv_wallclock=0;
fdb_error_t waitError(FDBFuture *f);

void *runNetwork(void *params);

FDBDatabase *openDatabase(pthread_t *netThread, std::string &args);


static fdb_error_t getError(fdb_error_t err, const char *context) {
    if (err) {
        char *msg = (char *) malloc(strlen(context) + 100);
        sprintf(msg, "Error in %s: %s", context, fdb_get_error(err));
        FATAL("%s\n", msg);
        free(msg);
    }

    return err;
}

static void checkError(fdb_error_t err, const char *context) {
    if (getError(err, context)) {
        FATAL("FDB Error %s", context);
        exit(1);
    }
}

void *runNetwork(void *params) {
    if (fdb_run_network()) {
        FATAL("FDB_RUN_NETWORK FAILED");
    } else {
        PRINT("AFAIK run network is fine");
    }
    return NULL;
}


FDBDatabase *openDatabase(pthread_t *netThread, std::string &cluster_file, std::string &args) {
    checkError(fdb_setup_network(), "setup network");
    pthread_create(netThread, NULL, &runNetwork, NULL);

#ifdef LOG_FDB
    checkError(fdb_network_set_option(FDB_NET_OPTION_TRACE_ENABLE, (uint8_t const *) "/mnt/ddi/", sizeof("/mnt/ddi")),
               "Net Option");
#endif
    //Note: unrecognized options are printed, but no error is thrown
    if (args != DEFAULT_ADDITIONAL_ARGS) {
        //From fdb_c_options.g
        // Set internal tuning or debugging knobs
        // Parameter: (String) knob_name=knob_value
        //FDB_NET_OPTION_KNOB=40,

        //If only one arg, go
        if (args.find(",") == std::string::npos) {
            PRINT_FORMAT("Setting single %s of len %zu", args.c_str(), strlen(args.c_str()));
            if (fdb_network_set_option(FDB_NET_OPTION_KNOB, (uint8_t const *) args.c_str(), strlen(args.c_str()))) {
                FATAL("Could not set option %s", args.c_str());
                exit(1);
            }
        } else {//Need to tokenize (We could just use the tokenize branch)
            char *ar = (char *) malloc(sizeof(char) * args.size() + 1);
            if (!ar) {
                FATAL("Cannot allocate temp buffer.");
                exit(1);
            }
            memcpy(ar, args.data(), args.size() + 1);
            char *p = strtok(ar, ",");
            while (p != nullptr) {
                PRINT_FORMAT("Setting %s of len %zu", p, strlen(p));
                if (fdb_network_set_option(FDB_NET_OPTION_KNOB, (uint8_t const *) p, strlen(p))) {
                    FATAL("Could not set option %s", p);
                    exit(1);
                }
                p = strtok(NULL, ",");
            }
            free(ar);
        }
    }
    const char *fdb = cluster_file.c_str();
    FDBDatabase *db;
    checkError(fdb_create_database(fdb, &db), "create database");

    PRINT_FORMAT("Trying to connect to server using %s", fdb);
    //PRINT_FORMAT("If you see you can't connect, be sure the IP is reachable")

    return db;
}

int run_fdb_op(fdb_op_g *op, FDBDatabase *db);


template<typename IO>
KVOrderedFDB<IO>::KVOrderedFDB(fkvb_test_conf *cconf) {
    conf = cconf;
};


#define SNAPSHOT_READ 1
#define SERIALIZABLE_READ 0

//TODO: Check the fdb_error_predicate( FDB_ERROR_PREDICATE_RETRYABLE) in the get
template<typename IO>
int
KVOrderedFDB<IO>::get(const char key[], size_t key_size, char *val_buff, size_t val_buff_size,
                      size_t &val_size_read, size_t &val_size) {
    op_params_get params = op_params_get((char *) &key[0], key_size, val_buff, val_buff_size);
    fdb_op_get op_get = fdb_op_get(&params);
    int rc = run_fdb_op(&op_get, db);
    if (rc) {
        TRACE_FORMAT("Error when reading %.*s", (int) key_size, key);
        return rc;
    }
    TRACE_FORMAT("GOT %.*s", (int) params.size_read, (char *) params.buf);
    val_size_read = params.size_read;
    val_size = params.val_read;
    return rc;
}

template<typename IO>
int KVOrderedFDB<IO>::shutdown() {
    //No-op. Needs to be killed from the outside
    //delete db;
    return 0;
}

#include <unistd.h>
#include <typeinfo>

template<typename IO>
int KVOrderedFDB<IO>::init() {
    PRINT_FORMAT("SETTING API %u", FDB_API_VERSION);
    checkError(fdb_select_api_version(FDB_API_VERSION), "select API version");
    PRINT_FORMAT("Running FKVB test at client version: %s", fdb_get_client_version());
    db = openDatabase(&_netThread, conf->config_file, conf->additional_args);
    if (db == nullptr) {
        FATAL("DB IS NULL");
    }

    /*
    PRINT_FORMAT("Clearing range");
    const int rc = run_fdb_op(new fdb_op_clearall(new op_params_clearall()), db);
    if (rc) {
        FATAL("ERROR in clearing range");
    }
    PRINT("RANGE cleared");
     */
    return 0;
}


template<typename IO>
int KVOrderedFDB<IO>::put_bulk(size_t numkv, char *k_ptr, size_t *k_sizes, char *v_ptrs, size_t *v_sizes) {

    /*
    size_t i;
    int rc;
    char *k = k_ptr, *v = v_ptrs;
    for (i = 0; i < numkv; i++) {
        rc = put(k, k_sizes[i], v, v_sizes[i]);
        k += k_sizes[i];
        v += v_sizes[i];
        if (rc)return rc;
    }
     */

    op_params_put_bulk params = op_params_put_bulk(k_ptr, k_sizes, v_ptrs, v_sizes, numkv);
    fdb_op_put_bulk op_put = fdb_op_put_bulk(&params);
    int ret;
    if (!(ret = run_fdb_op(&op_put, db))) {
        TRACE_FORMAT("PUT %s _ %s", key, val);
    } else {
        FATAL("ERROR in bulk put");
    }
    return ret;
}

template<typename IO>
int KVOrderedFDB<IO>::put(const char key[], size_t key_size, const char *val, size_t val_size) {
    op_params_put params = op_params_put((char *) &key[0], key_size, val, val_size);
    fdb_op_put op_put = fdb_op_put(&params);
    int ret;
    if (!(ret = run_fdb_op(&op_put, db))) {
        TRACE_FORMAT("PUT %s _ %s", key, val);
    } else {
        FATAL("ERROR while putting %s %s", key, val);
    }
    return ret;
}

template<typename IO>
int KVOrderedFDB<IO>::generic(int num_op, bool *rw, char *keys, size_t *key_sizes, char **put_values,
                              size_t *put_value_sizes, char *get_buffer, size_t get_buffer_size, size_t *read_values,
                              std::vector<char *> &read_values_ptr) {
    op_params_generic params = op_params_generic(num_op, rw, keys, key_sizes, put_values, put_value_sizes, get_buffer,
                                                 get_buffer_size, read_values, read_values_ptr, generic_futures);
    fdb_op_generic op = fdb_op_generic(&params);
    return run_fdb_op(&op, db);
}

template<typename IO>
int KVOrderedFDB<IO>::del(const char key[], size_t key_size) {
    NOT_IMPLEMENTED
    //We have to do implement it with the new structure
    FDBTransaction *tr = NULL;
    fdb_error_t e = fdb_database_create_transaction(db, &tr);
    checkError(e, "create transaction");
    unsigned int maxR = MAX_RETRY;
    while (maxR--) {
        TRACE_FORMAT("Deleting key %s with size", key, key_size,);
        fdb_transaction_clear(tr, (uint8_t const *) key, key_size);

        FDBFuture *f = fdb_transaction_commit(tr);
        e = waitError(f);
        fdb_future_destroy(f);


        if (e) {
            f = fdb_transaction_on_error(tr, e);
            fdb_error_t retryE = waitError(f);
            fdb_future_destroy(f);
            if (retryE) {
                fdb_transaction_destroy(tr);
                FATAL("Unexpected error while doing DEL")
            } else {
                //Going to loop over and retry the operation
            }
        } else {
            //Ok. Transaction committed
            fdb_transaction_destroy(tr);
            return 0;
        }
    }
    FATAL("Could not perform single DEL in %u attempts", MAX_RETRY);
}

template<typename IO>
unsigned long KVOrderedFDB<IO>::get_size() const {
    op_params_num_keys params = op_params_num_keys();

    fdb_op_num_keys num = fdb_op_num_keys(&params);
    u32 keys = 0;
    while (1) {
        int rc = run_fdb_op(&num, db);
        if (!rc) {
            PRINT_FORMAT("Number of keys is %zu", params.num_keys);
            break;
        } else {
            PRINT_FORMAT("SUMMING %zu", params.num_keys);
            keys += params.num_keys;
        }
    }
    return (unsigned long) params.num_keys;
    int ret;
    if (!(ret = run_fdb_op(&num, db))) {
        PRINT_FORMAT("Number of keys is %zu", params.num_keys);
    } else {
        FATAL("ERROR while counting number of keys");
    }
    return (unsigned long) params.num_keys;
}

template<typename IO>
unsigned long KVOrderedFDB<IO>::get_raw_capacity() const {
    NOT_IMPLEMENTED
}

template<typename IO>
void KVOrderedFDB<IO>::thread_local_entry() {
    generic_futures = (FDBFuture **) malloc(1024 * sizeof(FDBFuture * *));
    if (generic_futures == nullptr) {
        FATAL("Could not allocate generic futures");
    }
}

template<typename IO>
void KVOrderedFDB<IO>::thread_local_exit() {
    free(generic_futures);
}

template<typename IO>
int
KVOrderedFDB<IO>::get_range(const char start_key[], size_t start_key_size, const char end_key[],
                            size_t end_key_size,
                            char *kv_buff, size_t kv_buff_size, size_t &kv_size_read,
                            std::vector<char *> &kv_ptrs) {
    NOT_IMPLEMENTED
}


fdb_error_t waitError(FDBFuture *f) {
    fdb_error_t blockError = fdb_future_block_until_ready(f);
    if (!blockError) {
        return fdb_future_get_error(f);
    } else {
        return blockError;
    }
}


/*
https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_on_error
On receiving any type of error from an fdb_transaction_*() function, the application should:

1. Call fdb_transaction_on_error() with the returned fdb_error_t code. //here done by waitOnError
2. Wait for the resulting future to be ready.
3. If the resulting future is itself an error, destroy the future and FDBTransaction and report the error in an appropriate way.
4. If the resulting future is not an error, destroy the future and restart the application code that performs the transaction.
   The transaction itself will have already been reset to its initial state, but should not be destroyed and re-created
   because state used by fdb_transaction_on_error() to implement its backoff strategy and state related to timeouts and
   retry limits is stored there.
 */

#define GRV

#ifndef GRV
#error "We are always first requiring the GRV"
#endif

#if defined(LOG_FDB) and defined(LOG_TX)
//NB: apparently just setting the name suggests the runtime we want to profile the tx. So we should not even
//Set the id of the ROT if we do not want to trigger profiling
//checkError(fdb_transaction_set_option(tr, FDB_TR_OPTION_TRANSACTION_LOGGING_ENABLE, &one, 8),"LOG");
#define LOG_OP(tr) do{\
        snprintf(tx_sid,17,"TX_%d_%lu",tid,tx_id++);\
        fdb_error_t err = fdb_transaction_set_option(tr, FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER, (const uint8_t *) tx_sid, 17);\
        if (err) { FATAL("%s",fdb_get_error(err));}\
        err = fdb_transaction_set_option(tr, FDB_TR_OPTION_LOG_TRANSACTION, nullptr, 0);\
        if (err) { FATAL("%s",fdb_get_error(err));}\
        }while(0);
#else
#define LOG_OP(tr)
#endif

int run_fdb_op(fdb_op_g *op, FDBDatabase *db) {
    FDBTransaction *tr = NULL;
    fdb_error_t e = fdb_database_create_transaction(db, &tr);
    checkError(e, "create transaction");
    //const uint8_t limit = 5;
    /*if (fdb_transaction_set_option(tr, FDB_TR_OPTION_RETRY_LIMIT, (const uint8_t *) &limit, sizeof(uint64_t))) {
        FATAL("COULD NOT SET RETRY LIMIT TO TX");
    }*/

    LOG_OP(tr);

//NB: In case of failure, grv and commit time are taken only for the successful run
    while (1) {
#ifdef GRV
        //take time
        fdb_error_t grv_e = 0;
        uint64_t init, end;
        init = ticks::get_ticks();
	//TODO, FIXME:  if we get a tx_too_old error, we should reset the grv no matter what the cache says
	//cache_grv has been translated to ticks
	//it is 0 if disabled
	bool get_grv=(init-last_grv_wallclock)>grv_cache_tics_ms; 
	if(get_grv){
		//Get a grv from FDB and extract the value
		do {
		    FDBFuture *grv_f = fdb_transaction_get_read_version(tr);
		    grv_e = fdb_future_block_until_ready(grv_f);
		    //get the grv. This is done in any case
		    if(fdb_future_get_int64(grv_f, &last_grv))FATAL("Getting grv value out of future gave error\n");
		    fdb_future_destroy(grv_f);
		} while (grv_e);
	}else{
		//Use the last grv
		fdb_transaction_set_read_version(tr,last_grv);
	}
	end = ticks::get_ticks();
	if(get_grv)last_grv_wallclock = end;
        zrl_fkvb_begin_latency =end-init; //In case of retries, we consider the init time as the sum of all attempts
#endif
        struct op_result result = op->run(tr);
        e = result.err;
        if (!e) {
            init = ticks::get_ticks();
            if (!op->is_ro()) {//Do not commit generic
		    FDBFuture *f = fdb_transaction_commit(tr);
		    e = waitError(f);
		    fdb_future_destroy(f);
            }
            zrl_fkvb_commit_latency = ticks::get_ticks() - init;
        }

        /*
         * From https://apple.github.io/foundationdb/api-c.html
         * On receiving any type of error from an fdb_transaction_*() function, the application should:
         * Call fdb_transaction_on_error() with the returned fdb_error_t code.
         * Wait for the resulting future to be ready.
         * If the resulting future is itself an error, destroy the future and FDBTransaction and report the error in an appropriate way.
         * If the resulting future is not an error, destroy the future and restart the application code that performs the transaction.
         * The transaction itself will have already been reset to its initial state, but should not be destroyed and re-created
         * because state used by fdb_transaction_on_error() to implement its backoff strategy and state related to timeouts and retry limits is stored there.
         */
        if (e) {//Error in the operation OR error in the commit
            FDBFuture *f = fdb_transaction_on_error(tr, e);
            fdb_error_t retryE = waitError(f);
            fdb_future_destroy(f);
            if (retryE) {
                fdb_transaction_destroy(tr);
#if 0
                static const char *p = "put_op_bulk";
                if (std::string(op->str()) == std::string(p)) {
                    op_params_put_bulk *pbulk = ((fdb_op_put_bulk *) op)->params;
                    unsigned int k;
                    unsigned char *key = (unsigned char *) pbulk->keys;
                    for (k = 0; k < pbulk->num_ops; k++) {
                        unsigned int kk;
                        for (kk = 0; kk < pbulk->key_sizes[k]; kk++) {
                            printf("%hu.", key[kk]);
                        }
                        printf("\n");
                        key += pbulk->key_sizes[k];
                    }
                }
#endif
                FATAL("A non-retriable error occurred on op %s", op->str());

            } else {
                //Loop over and retry
                TRACE_FORMAT("WARNING: Retrying op %s", op->str());
            }
        } else {//No FDB errors, but check for app-level errors
            fdb_transaction_destroy(tr);
            return result.rc;
        }
    }

}

//Template instantiation. This goes in the cc file
template
class KVOrderedFDB<int>;
