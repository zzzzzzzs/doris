// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "txn_kv_error.h"

// =============================================================================
//
// =============================================================================

namespace doris::cloud {

class Transaction;
class RangeGetIterator;

class TxnKv {
public:
    TxnKv() = default;
    virtual ~TxnKv() = default;

    /**
     * Creates a transaction
     * TODO: add options to create the txn
     *
     * @param txn output param
     * @return TXN_OK for success
     */
    virtual TxnErrorCode create_txn(std::unique_ptr<Transaction>* txn) = 0;

    virtual int init() = 0;
};

class Transaction {
public:
    Transaction() = default;
    virtual ~Transaction() = default;

    virtual void put(std::string_view key, std::string_view val) = 0;

    /**
     * @param snapshot if true, `key` will not be included in txn conflict detection this time
     * @return TXN_OK for success get a key, TXN_KEY_NOT_FOUND for key not found, otherwise for error
     */
    virtual TxnErrorCode get(std::string_view key, std::string* val, bool snapshot = false) = 0;

    /**
     * Closed-open range
     * @param snapshot if true, key range will not be included in txn conflict detection this time
     * @param limit if non-zero, indicates the maximum number of key-value pairs to return
     * @return TXN_OK for success, otherwise for error
     */
    virtual TxnErrorCode get(std::string_view begin, std::string_view end,
                             std::unique_ptr<RangeGetIterator>* iter, bool snapshot = false,
                             int limit = 10000) = 0;

    /**
     * Put a key-value pair in which key will in the form of
     * `key_prefix + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key_prefix prefix for key convertion, can be zero-length
     * @param val value
     */
    virtual void atomic_set_ver_key(std::string_view key_prefix, std::string_view val) = 0;

    /**
     * Put a key-value pair in which key will in the form of
     * `value + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key prefix for key convertion, can be zero-length
     * @param val value
     */
    virtual void atomic_set_ver_value(std::string_view key, std::string_view val) = 0;

    /**
     * Adds a value to database
     * @param to_add positive for addition, negative for substraction
     * @return 0 for success otherwise error
     */
    virtual void atomic_add(std::string_view key, int64_t to_add) = 0;
    // TODO: min max or and xor cmp_and_clear set_ver_value

    virtual void remove(std::string_view key) = 0;

    /**
     * Remove a closed-open range
     */
    virtual void remove(std::string_view begin, std::string_view end) = 0;

    /**
     *
     *@return TXN_OK for success otherwise error
     */
    virtual TxnErrorCode commit() = 0;

    /**
     * Gets the read version used by the txn.
     * Note that it does not make any sense we call this function before
     * any `Transaction::get()` is called.
     *
     *@return TXN_OK for success otherwise error
     */
    virtual TxnErrorCode get_read_version(int64_t* version) = 0;

    /**
     * Gets the commited version used by the txn.
     * Note that it does not make any sense we call this function before
     * a successful call to `Transaction::commit()`.
     *
     *@return TXN_OK for success, TXN_CONFLICT for conflict, otherwise error
     */
    virtual TxnErrorCode get_committed_version(int64_t* version) = 0;

    /**
     * Aborts this transaction
     *
     * @return TXN_OK for success otherwise error
     */
    virtual TxnErrorCode abort() = 0;

    struct BatchGetOptions {
        BatchGetOptions() : BatchGetOptions(false) {};
        BatchGetOptions(bool s) : snapshot(s), concurrency(1000) {};

        // if true, `key` will not be included in txn conflict detection this time.
        //
        // Default: false
        bool snapshot;

        // the maximum number of concurrent requests submitted to fdb at one time.
        //
        // Default: 1000
        int concurrency;
    };
    /**
     * @brief batch get keys
     *
     * @param res
     * @param keys
     * @param opts
     * @return If all keys are successfully retrieved, return TXN_OK. Otherwise, return the code of the first occurring error
     */
    virtual TxnErrorCode batch_get(std::vector<std::optional<std::string>>* res,
                                   const std::vector<std::string>& keys,
                                   const BatchGetOptions& opts = BatchGetOptions()) = 0;

    /**
     * @brief return the approximate bytes consumed by the underlying transaction buffer.
     **/
    virtual size_t approximate_bytes() const = 0;

    /**
     * @brief return the num delete keys submitted to this txn.
     **/
    virtual size_t num_del_keys() const = 0;

    /**
     * @brief return the num put keys submitted to this txn.
     **/
    virtual size_t num_put_keys() const = 0;

    /**
     * @brief return the bytes of the delete keys consumed.
     **/
    virtual size_t delete_bytes() const = 0;

    /**
     * @brief return the bytes of the put key and values consumed.
     **/
    virtual size_t put_bytes() const = 0;
};

class RangeGetIterator {
public:
    RangeGetIterator() = default;
    virtual ~RangeGetIterator() = default;

    /**
     * Checks if we can call `next()` on this iterator.
     */
    virtual bool has_next() = 0;

    /**
     * Gets next element, this is usually called after a check of `has_next()` succeeds,
     * If `has_next()` is not checked, the return value may be undefined.
     *
     * @return a kv pair
     */
    virtual std::pair<std::string_view, std::string_view> next() = 0;

    /**
     * Repositions the offset to `pos`
     */
    virtual void seek(size_t pos) = 0;

    /**
     * Checks if there are more KVs to be get from the range, caller usually wants
     * to issue another `get` with the last key of this iteration.
     *
     * @return if there are more kvs that this iterator cannot cover
     */
    virtual bool more() = 0;

    /**
     * 
     * Gets size of the range, some kinds of iterators may not support this function.
     *
     * @return size
     */
    virtual int size() = 0;

    /**
     * Resets to initial state, some kinds of iterators may not support this function.
     */
    virtual void reset() = 0;

    /**
     * Get the begin key of the next iterator if `more()` is true, otherwise returns empty string.
     */
    virtual std::string next_begin_key() = 0;

    RangeGetIterator(const RangeGetIterator&) = delete;
    RangeGetIterator& operator=(const RangeGetIterator&) = delete;
};

// =============================================================================
//  FoundationDB implementation of TxnKv
// =============================================================================

namespace fdb {
class Database;
class Transaction;
class Network;
} // namespace fdb

class FdbTxnKv : public TxnKv {
public:
    FdbTxnKv() = default;
    ~FdbTxnKv() override = default;

    TxnErrorCode create_txn(std::unique_ptr<Transaction>* txn) override;

    int init() override;

private:
    std::shared_ptr<fdb::Network> network_;
    std::shared_ptr<fdb::Database> database_;
};

namespace fdb {

class Network {
public:
    Network(FDBNetworkOption opt) : opt_(opt) {}

    /**
     * @return 0 for success otherwise non-zero
     */
    int init();

    /**
     * Notify the newwork thread to stop, this is an async. call, check
     * Network::working to ensure the network exited finally.
     *
     * FIXME: may be we can implement it as a sync. function.
     */
    void stop();

    ~Network() = default;

private:
    std::shared_ptr<std::thread> network_thread_;
    FDBNetworkOption opt_;

    // Global state, only one instance of Network is allowed
    static std::atomic<bool> working;
};

class Database {
public:
    Database(std::shared_ptr<Network> net, std::string cluster_file, FDBDatabaseOption opt)
            : network_(std::move(net)), cluster_file_path_(std::move(cluster_file)), opt_(opt) {}

    /**
     *
     * @return 0 for success otherwise false
     */
    int init();

    ~Database() {
        if (db_ != nullptr) fdb_database_destroy(db_);
    }

    FDBDatabase* db() { return db_; };

    std::shared_ptr<Transaction> create_txn(FDBTransactionOption opt);

private:
    std::shared_ptr<Network> network_;
    std::string cluster_file_path_;
    FDBDatabase* db_ = nullptr;
    FDBDatabaseOption opt_;
};

class RangeGetIterator : public cloud::RangeGetIterator {
public:
    /**
     * Iterator takes the ownership of input future
     */
    RangeGetIterator(FDBFuture* fut, bool owns = true)
            : fut_(fut), owns_fut_(owns), kvs_(nullptr), kvs_size_(-1), more_(false), idx_(-1) {}

    RangeGetIterator(RangeGetIterator&& o) {
        if (fut_ && owns_fut_) fdb_future_destroy(fut_);
        fut_ = o.fut_;
        owns_fut_ = o.owns_fut_;
        kvs_ = o.kvs_;
        kvs_size_ = o.kvs_size_;
        more_ = o.more_;
        idx_ = o.idx_;

        o.fut_ = nullptr;
        o.kvs_ = nullptr;
        o.idx_ = 0;
        o.kvs_size_ = 0;
        o.more_ = false;
    }

    ~RangeGetIterator() override {
        // Release all memory
        if (fut_ && owns_fut_) fdb_future_destroy(fut_);
    }

    TxnErrorCode init();

    std::pair<std::string_view, std::string_view> next() override {
        if (idx_ < 0 || idx_ >= kvs_size_) return {};
        const auto& kv = kvs_[idx_++];
        return {{(char*)kv.key, (size_t)kv.key_length}, {(char*)kv.value, (size_t)kv.value_length}};
    }

    void seek(size_t pos) override { idx_ = pos; }

    bool has_next() override { return (idx_ < kvs_size_); }

    /**
     * Check if there are more KVs to be get from the range, caller usually wants
     * to issue a nother `get` with the last key of this iteration.
     */
    bool more() override { return more_; }

    int size() override { return kvs_size_; }

    void reset() override { idx_ = 0; }

    std::string next_begin_key() override {
        std::string k;
        if (!more()) return k;
        const auto& kv = kvs_[kvs_size_ - 1];
        k.reserve((size_t)kv.key_length + 1);
        k.append((char*)kv.key, (size_t)kv.key_length);
        k.push_back('\x00');
        return k;
    }

    RangeGetIterator(const RangeGetIterator&) = delete;
    RangeGetIterator& operator=(const RangeGetIterator&) = delete;

private:
    FDBFuture* fut_;
    bool owns_fut_;
    const FDBKeyValue* kvs_;
    int kvs_size_;
    fdb_bool_t more_;
    int idx_;
};

class Transaction : public cloud::Transaction {
public:
    friend class Database;
    Transaction(std::shared_ptr<Database> db) : db_(std::move(db)) {}

    ~Transaction() override {
        if (txn_) fdb_transaction_destroy(txn_);
    }

    /**
     *
     * @return TxnErrorCode for success otherwise false
     */
    TxnErrorCode init();

    void put(std::string_view key, std::string_view val) override;

    using cloud::Transaction::get;
    /**
     * @param snapshot if true, `key` will not be included in txn conflict detection this time
     * @return TXN_OK for success get a key, TXN_KEY_NOT_FOUND for key not found, otherwise for error
     */
    TxnErrorCode get(std::string_view key, std::string* val, bool snapshot = false) override;
    /**
     * Closed-open range
     * @param snapshot if true, key range will not be included in txn conflict detection this time
     * @param limit if non-zero, indicates the maximum number of key-value pairs to return
     * @return TXN_OK for success, otherwise for error
     */
    TxnErrorCode get(std::string_view begin, std::string_view end,
                     std::unique_ptr<cloud::RangeGetIterator>* iter, bool snapshot = false,
                     int limit = 10000) override;

    /**
     * Put a key-value pair in which key will in the form of
     * `key_prefix + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key_prefix prefix for key convertion, can be zero-length
     * @param val value
     */
    void atomic_set_ver_key(std::string_view key_prefix, std::string_view val) override;

    /**
     * Put a key-value pair in which key will in the form of
     * `value + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key prefix for key convertion, can be zero-length
     * @param val value
     */
    void atomic_set_ver_value(std::string_view key, std::string_view val) override;

    /**
     * Adds a value to database
     * @param to_add positive for addition, negative for substraction
     */
    void atomic_add(std::string_view key, int64_t to_add) override;
    // TODO: min max or and xor cmp_and_clear set_ver_value

    void remove(std::string_view key) override;

    /**
     * Remove a closed-open range
     */
    void remove(std::string_view begin, std::string_view end) override;

    /**
     *
     *@return TXN_OK for success, TXN_CONFLICT for conflict, otherwise for error
     */
    TxnErrorCode commit() override;

    TxnErrorCode get_read_version(int64_t* version) override;
    TxnErrorCode get_committed_version(int64_t* version) override;

    TxnErrorCode abort() override;

    TxnErrorCode batch_get(std::vector<std::optional<std::string>>* res,
                           const std::vector<std::string>& keys,
                           const BatchGetOptions& opts = BatchGetOptions()) override;

    size_t approximate_bytes() const override { return approximate_bytes_; }

    size_t num_del_keys() const override { return num_del_keys_; }

    size_t num_put_keys() const override { return num_put_keys_; }

    size_t delete_bytes() const override { return delete_bytes_; }

    size_t put_bytes() const override { return put_bytes_; }

private:
    std::shared_ptr<Database> db_ {nullptr};
    bool commited_ = false;
    bool aborted_ = false;
    FDBTransaction* txn_ = nullptr;

    size_t num_del_keys_ {0};
    size_t num_put_keys_ {0};
    size_t delete_bytes_ {0};
    size_t put_bytes_ {0};
    size_t approximate_bytes_ {0};
};

} // namespace fdb
} // namespace doris::cloud
