/*
 *  Copyright (c) 2021 International Business Machines
 *  All rights reserved.
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Authors: Diego Didona (ddi@zurich.ibm.com),
 *
 */

#ifndef FDB_HELPER_HH
#define FDB_HELPER_HH

#include "defs.hh"


#include <ticks.hh>

static const int MAX_KEY_SIZE = 2048;
extern thread_local char tx_sid[20];


//We use the params also for return value
struct op_result {
    int rc;
    fdb_error_t err;
    uint64_t grv_time, commit_time;

    op_result(int r, fdb_error_t e) : rc(r), err(e), grv_time(0), commit_time(0) {}

    op_result(int r, fdb_error_t e, uint64_t grv, uint64_t com) : rc(r), err(e), grv_time(grv), commit_time(com) {}

    op_result(int r, fdb_error_t e, uint64_t grv) : rc(r), err(e), grv_time(grv), commit_time(0) {}
};

struct op_params {

};

struct op_params_get : public op_params {
    char *key;
    size_t key_size;
    char *buf;
    size_t buf_size;
    size_t size_read;
    size_t val_read;


    op_params_get(char *_key, size_t _key_size, char *_buf, size_t _buf_size) :
            key(_key), key_size(_key_size), buf(_buf), buf_size(_buf_size) {}
};

struct op_params_clearall : public op_params {
    op_params_clearall() {}
};

struct op_params_num_keys : public op_params {
    //https://stackoverflow.com/questions/20944784/why-is-conversion-from-string-constant-to-char-valid-in-c-but-invalid-in-c
    char const init_key = '\0';//"0";
    char const end_key = '\xFF';
    static const size_t offset = 1 << 10;  //MUST be power of two
    size_t num_keys = 0;

    char last_key[MAX_KEY_SIZE];
    int last_size;


    op_params_num_keys() {
        //We need a power-of-two offset b/c we halve it to find the size of the db
        if ((!offset) || (offset & (offset - 1))) {
            FATAL("Offset not power of 2: %zu", offset);
        }
        memcpy(last_key, &init_key, sizeof(init_key));
        last_size = sizeof(init_key);
    }

};

struct op_params_generic : public op_params {
    int num_op;
    bool *rw;
    char *keys;
    size_t *key_sizes;
    char **put_values;
    size_t *put_value_sizes;
    char *get_buffer;
    size_t get_buffer_size;
    size_t *read_values;
    std::vector<char *> read_values_ptr;
    FDBFuture **futures;

    op_params_generic(int _num_op, bool *_rw, char *_keys, size_t *_key_sizes, char **_put_values,
                      size_t *_put_value_sizes, char *_get_buffer, size_t _get_buffer_size, size_t *_read_values,
                      std::vector<char *> &_read_values_ptr, FDBFuture **_futures) :
            num_op(_num_op),
            rw(_rw),
            keys(_keys),
            key_sizes(_key_sizes),
            put_values(_put_values),
            put_value_sizes(_put_value_sizes),
            get_buffer(_get_buffer),
            get_buffer_size(_get_buffer_size),
            read_values(_read_values),
            read_values_ptr(_read_values_ptr),
            futures(_futures) {}

};

struct op_params_put : public op_params {
    const char *key;
    size_t key_size;
    const char *value;
    size_t value_size;

    op_params_put(const char *_key, size_t _key_size, const char *_buf, size_t _buf_size) :
            key(_key), key_size(_key_size), value(_buf), value_size(_buf_size) {};
};


struct op_params_put_bulk : public op_params {
    const char *keys;
    size_t *key_sizes;
    const char *values;
    size_t *value_sizes;
    size_t num_ops;

    op_params_put_bulk(const char *_keys, size_t *_key_sizes, const char *_buf, size_t *_buf_size, size_t num) :
            keys(_keys), key_sizes(_key_sizes), values(_buf), value_sizes(_buf_size), num_ops(num) {};
};

enum fdb_ops {
    GRV = 0, COMMIT = 1, GENERIC=2, GET=3, PUT,DELETE=4, CLEAR_ALL=5, GET_NUM_KEYS=6, PUT_BULK=7
};

struct fdb_op_g {
    virtual op_result run(FDBTransaction *tr) = 0;

    virtual const char *str() = 0;
    virtual const fdb_ops op() = 0;
    virtual const bool is_ro()=0;
};


template<typename param_type>
struct fdb_op : public fdb_op_g {
    param_type *params;
    u64 cache_grv;

    fdb_op(param_type *p) : params(p),cache_grv(0)  {};
    fdb_op(param_type *p,u64 cachegrv) : params(p), cache_grv(cachegrv){};

    virtual op_result run(FDBTransaction *tr) = 0;

    virtual const char *str() = 0;

    virtual const fdb_ops op() = 0;
    virtual const bool is_ro()=0;

};

#define SNAPSHOT_READ 1
#define SERIALIZABLE_READ 0

struct fdb_op_clearall : public fdb_op<op_params_clearall> {
    fdb_op_clearall(op_params_clearall *p) : fdb_op<op_params_clearall>(p) {}

    op_result run(FDBTransaction *tr) {
        fdb_transaction_clear_range(tr, (uint8_t *) "", 0, (uint8_t *) "\xff", 1);
        return op_result(0, 0);
    }

    const char *str() { return "clear_all"; }

    const fdb_ops op() { return CLEAR_ALL; }


    const bool is_ro(){return false;}
};

/*
 * Note for self. RYWTransactions are the one used. ThreadSafeTransactions are the wrapper
 */
struct fdb_op_generic : public fdb_op<op_params_generic> {
    fdb_op_generic(op_params_generic *p) : fdb_op<op_params_generic>(p) {}

    const char *str() { return "generic_op"; }

    const fdb_ops op() { return GENERIC; }

    const bool is_ro(){
	    if( params->num_op ==0) return true; 
	    int i;
	    for(i=0;i<params->num_op;i++){
	        if(params->rw[i]) return false;
	    }
	    return true;    	    
    }
    op_result run(FDBTransaction *tr) {
        int done = 0;
        int put_index = 0;
        int rc = 0;
        char *value_ptr = params->put_values[0], *key_ptr = params->keys;
        while (done < params->num_op) {
            if (params->rw[done]) {
                TRACE_FORMAT("%d PUTTING key %.*s on address(%p) with size %zu and  value %.*s", done,
                             (int) params->key_sizes[done], key_ptr, value_ptr, params->put_value_sizes[put_index],
                             (int) params->put_value_sizes[put_index], value_ptr);
                fdb_transaction_set(tr, (uint8_t *) key_ptr, params->key_sizes[done],
                                    (uint8_t *) value_ptr, params->put_value_sizes[put_index]);
                put_index++;
                value_ptr = params->put_values[put_index];//params->put_value_sizes[put_index];

            } else {
                TRACE_FORMAT("%s %d GETTING key %.*s", tx_sid, done, (int) params->key_sizes[done], key_ptr);
                params->futures[done] = fdb_transaction_get(tr, (uint8_t *) key_ptr,
                                                            params->key_sizes[done], SERIALIZABLE_READ);
            }
            key_ptr += params->key_sizes[done];
            done++;
        }
        done = 0;
        key_ptr = params->keys;

        while (done < params->num_op) {
            if (!params->rw[done]) {
                TRACE_FORMAT("%d GET CHECK", done);
                FDBFuture *f = params->futures[done];
                fdb_error_t e = fdb_future_block_until_ready(f);
                if (e) {
                    fdb_future_destroy(f);
                    return op_result(0, e);
                }
                fdb_bool_t present;
                uint8_t const *outValue;
                int outValueLength;

                e = fdb_future_get_value(f, &present, &outValue, &outValueLength);
                fdb_future_destroy(f);

                if (e) {
                    return op_result(0, e);
                } else {
                    if (!present) {
                        PRINT_FORMAT("WARNING: Value not found for key %.*s", (int) params->key_sizes[done], key_ptr);
                        rc = 2;
                    } else {
                        TRACE_FORMAT("Read %s length %d", outValue, outValueLength);
                    }
                }
                //ALL good, now we have to copy results to user supplied buffers
                // FIXME @ddi: copy to usr buffer
                TRACE_FORMAT("%.*s => %.*s", (int) params->key_sizes[done], key_ptr, outValueLength, (char *) outValue);
            }
            key_ptr += params->key_sizes[done];
            done++;
        }
        return op_result(rc, 0);
    }

};

/*
   Solution inspired from the discussion at https://forums.foundationdb.org/t/getting-the-number-of-key-value-pairs/189/4
   Check also https://apple.github.io/foundationdb/developer-guide.html#key-selectors
   We use the get_key(k,offset) to get the single key at k+offset.
   We call the function from the start of the keyspace ('') until we get to the end of the range (special key '  ) and
   we sum the number of scanned keys in the meanwhile

   Note that an iteration of this algorithm may fail (e.g., a tx with many get_key can last way more than 5 sec).
   What we do is: we save the last key we tried to get and the number of keys read so far, and we restart a new transaction
   from that point

   Note that the restarting is transparent because it is done by the "run" function that we implement in the FDBKVstore
   (the errors returned in case a tx fail in this case are not critical, so the tx is retried transparently to this code)
 */
struct fdb_op_num_keys : public fdb_op<op_params_num_keys> {
//#define RETRY_GETCOUNT
    fdb_op_num_keys(op_params_num_keys *p) : fdb_op<op_params_num_keys>(p) {}

    const char *str() { return "get_num_keys"; }

    const fdb_ops op() { return GET_NUM_KEYS; }

    op_result run(FDBTransaction *tr) {
        FDBFuture *f, *f_old = nullptr;
        uint8_t *last = (uint8_t * ) & params->last_key[0];// & params->init_key;
        uint8_t *out_value;
        int last_size = params->last_size;
        size_t it = 0;

        TRACE_FORMAT("GET_KEYS INIT: last %.*s of size %d", last_size, (char *) last, last_size);

        size_t off = params->offset;

        while (1) {
            TRACE_FORMAT("ITERATION %zu start %.*s Offset %zu", it, last_size, last, off);

            f = fdb_transaction_get_key(tr, (const uint8_t *) last, last_size, true, off, SNAPSHOT_READ);

            fdb_error_t e = fdb_future_block_until_ready(f);
            if (e) {
                TRACE_FORMAT("ERROR IN TX GETTING FUTURE");

                //Remember to copy this before destroying the future, which holds the content of the variables
                memcpy(params->last_key, last, last_size);
                params->last_size = last_size;
                TRACE_FORMAT("NEXT last is going to be %.*s", (int) last_size, params->last_key);
                fdb_future_destroy(f);
                if (f_old != nullptr)fdb_future_destroy(f_old);
                return op_result(0, e);
            }

            int outValueLength;

            e = fdb_future_get_key(f, (const uint8_t **) &out_value, &outValueLength);
            if (e) {
                TRACE_FORMAT("ERROR IN FUTURE GETTING KEY");
                //Remember to copy this before destroying the future, which holds the content of the variables
                memcpy(params->last_key, last, last_size);
                params->last_size = last_size;
                TRACE_FORMAT("NEXT last is going to be %.*s", (int) last_size, params->last_key);

                fdb_future_destroy(f);
                if (f_old != nullptr)fdb_future_destroy(f_old);

                return op_result(0, e);
            }


            if ((int) *out_value == 255) {//This is the end key apparently.
                //We have reached the end. There are two cases
                //1. We are out of range because our offset was too large. Then we halve the offset and continue
                //2. We have really reached the end. This condition is true if offset is 1. Then we return
                if (off == 1) {
                    TRACE_FORMAT("End of scan at iteration %zu with %zu keys", it, params->num_keys);
                    fdb_future_destroy(f);
                    if (f_old != nullptr)fdb_future_destroy(f_old);
                    return op_result(0, 0);
                } else {
                    TRACE_FORMAT("Reached end of store. Halving offset");
                    off = off >> 1;
                    //We keep the last and last_size at the current value
                }
            } else {
                TRACE_FORMAT("Out key %.*s of size %d", outValueLength, (char *) out_value, outValueLength);
                if (outValueLength == 1) {
                    PRINT_FORMAT("actually a char %c %d", *out_value, (int) *out_value);
                }
                last = out_value;
                last_size = outValueLength;
                params->num_keys += off;
                TRACE_FORMAT("Keys now %zu", params->num_keys);
            }

            f_old = f;
            ++it;
            //We need to be able to reference last and last_size, which belong to the future of the just-past invocation
            //So we do not destroy it until we advance to a new base key

        }
    }

    const bool is_ro(){return true;}
};

struct fdb_op_get : public fdb_op<op_params_get> {

    fdb_op_get(op_params_get *p) : fdb_op<op_params_get>(p) {};

    const char *str() { return "get_op"; }

    const fdb_ops op() { return GET; }

    op_result run(FDBTransaction *tr) {
        int rc = 0;
        FDBFuture *f = fdb_transaction_get(tr, (uint8_t *) params->key, params->key_size, SERIALIZABLE_READ);


        fdb_error_t e = fdb_future_block_until_ready(f);
        if (e) {
            fdb_future_destroy(f);
            return op_result(0, e);
        }
        fdb_bool_t present;
        uint8_t const *outValue;
        int outValueLength;

        e = fdb_future_get_value(f, &present, &outValue, &outValueLength);
        fdb_future_destroy(f);

        if (e) {
            return op_result(0, e);
        }

        if (!present) {
            PRINT_FORMAT("WARNING: Value not found for key %.*s", (int) params->key_size, params->key);
            rc = 2;
        } else {
            //ALL good, now we have to copy results to user supplied buffers
            TRACE_FORMAT("Get key %s value %s size %d", params->key, outValue, outValueLength);
            params->val_read = outValueLength;
            params->size_read = (size_t)
                                        outValueLength > params->buf_size ? params->buf_size : outValueLength;
            memcpy(params->buf, outValue, params->size_read);
        }
        return op_result(rc, 0);
    }

    const bool is_ro(){return true;}
};

struct fdb_op_put : public fdb_op<op_params_put> {

    fdb_op_put(op_params_put *p) : fdb_op<op_params_put>(p) {}; //fdb_op_put(op_params_put p) : fdb_op(p) {}

    const char *str() { return "put_op"; }
    const fdb_ops op() { return PUT; }
    op_result run(FDBTransaction *tr) {
        TRACE_FORMAT("PUTTING key %.*s %.*s size %zu", (int) params->key_size, params->key, (int) params->value_size,
                     params->value, params->value_size);
        fdb_transaction_set(tr, (uint8_t * ) & params->key[0], params->key_size, (uint8_t *) params->value,
                            params->value_size);
        return op_result(0, 0);
    }

    const bool is_ro(){return false;}
};

struct fdb_op_put_bulk : public fdb_op<op_params_put_bulk> {

    fdb_op_put_bulk(op_params_put_bulk *p) : fdb_op<op_params_put_bulk>(
            p) {}; //fdb_op_put(op_params_put p) : fdb_op(p) {}

    const char *str() { return "put_op_bulk"; }

    const fdb_ops op() { return PUT_BULK; }

    const bool is_ro(){return false;}
    op_result run(FDBTransaction *tr) {
        u32 i;
        char *k = (char *) params->keys, *v = (char *) params->values;
        for (i = 0; i < params->num_ops; i++) {

            TRACE_FORMAT("PUTTING key %.*s %.*s size %zu", (int) params->key_size, params->key,
                         (int) params->value_size,
                         params->value, params->value_size);

            /*
             * Setting flags. Done every time bc apparently doing set resets the flags
             * https://forums.foundationdb.org/t/best-practices-for-bulk-load/422/6
             */
            fdb_error_t err = fdb_transaction_set_option(tr, FDB_TR_OPTION_NEXT_WRITE_NO_WRITE_CONFLICT_RANGE,
                                                         nullptr, 0);
            if (err) {
                FATAL("%s", fdb_get_error(err));
            }

            err = fdb_transaction_set_option(tr, FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE,
                                                         nullptr, 0);
            if (err) {
                FATAL("%s", fdb_get_error(err));
            }

            fdb_transaction_set(tr, (uint8_t *) k, params->key_sizes[i], (uint8_t *) v,
                                params->value_sizes[i]);
            k += params->key_sizes[i];
            v += params->value_sizes[i];
        }

        return op_result(0, 0);
    }
};


#endif //FDB_HELPER_HH
