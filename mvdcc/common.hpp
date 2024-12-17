#ifndef COMMON_HPP
#define COMMON_HPP

#include "config.hpp"
#include <vector>
#include <queue>
#include <cstdint>
#include <mutex>
#include <optional>
#include <functional>
#include <condition_variable>
#include <iostream>
#include <algorithm>

// Result Class: Store Transaction Results Per Thread
class Result {
public:
    uint64_t commit_cnt_ = 0; // Number of Committed Transactions
};

// Task Class: Define Transaction Tasks
enum class Ope { READ, WRITE };

class Task {
public:
    Ope ope_;
    uint64_t key_;

    Task(Ope ope, uint64_t key) : ope_(ope), key_(key) {}
};

// Transaction Class: Transaction Data
class Transaction {
public:
    uint64_t timestamp_;
    std::vector<std::pair<uint64_t, uint64_t>> read_set_;
    std::vector<uint64_t> write_set_;
    std::vector<Task> task_set_; // Task Ope List

    Transaction(uint64_t timestamp) : timestamp_(timestamp) {}
    Transaction() : timestamp_(0) {}
};

// Tuple Class: Records in the Database
class Tuple {
public:
    struct Version {
        uint64_t begin_timestamp_;
        uint64_t end_timestamp_;
        uint64_t value_;
        bool placeholder_;
        Version* prev_pointer_;

        Version(uint64_t begin, uint64_t end, uint64_t value, bool placeholder, Version* prev)
            : begin_timestamp_(begin), end_timestamp_(end), value_(value),
              placeholder_(placeholder), prev_pointer_(prev) {}
    };

    Version* latest_version_;

    Tuple() {
        latest_version_ = new Version(0, UINT64_MAX, 0, false, nullptr);
    }

    void addPlaceholder(uint64_t timestamp) {
        auto new_version = new Version(timestamp, UINT64_MAX, 0, true, latest_version_);
        if (latest_version_) {
            latest_version_->end_timestamp_ = timestamp;
        }
        latest_version_ = new_version;
    }

    bool updatePlaceholder(uint64_t timestamp, uint64_t value) {
        Version* version = latest_version_;
        while (version) {
            if (version->begin_timestamp_ == timestamp && version->placeholder_) {
                version->value_ = value;
                version->placeholder_ = false;
                return true;
            }
            version = version->prev_pointer_;
        }
        return false;
    }

    std::optional<uint64_t> getVersion(uint64_t timestamp) {
        Version* version = latest_version_;
        while (version) {
            if (version->begin_timestamp_ <= timestamp && timestamp < version->end_timestamp_ && !version->placeholder_) {
                return version->value_;
            }
            version = version->prev_pointer_;
        }
        return std::nullopt;
    }
};

// Global Variable Definition
extern std::vector<Tuple> Table;
extern std::vector<Transaction> transactions;
extern std::priority_queue<Transaction, std::vector<Transaction>, 
                           std::function<bool(const Transaction&, const Transaction&)>> ready_queue;
extern uint64_t tx_counter;
extern std::vector<std::vector<uint64_t>> thread_partitions;
extern std::mutex partition_mutex;
extern std::condition_variable ready_queue_cv;

// Common Function Definition
void makeDB(size_t tuple_num) {
    Table.resize(tuple_num);
    for (size_t i = 0; i < tuple_num; ++i) {
        Table[i] = Tuple();
    }
}

void initializeTransactions(size_t tuple_num, double read_ratio) {
    transactions.resize(tuple_num);
    for (uint64_t i = 0; i < tuple_num; ++i) {
        transactions[i] = Transaction(i);
        transactions[i].task_set_.clear(); // Initialize

        size_t task_num = 10; 
        for (size_t j = 0; j < task_num; ++j) {
            double rand_value = static_cast<double>(rand()) / RAND_MAX; // 0.0 ~ 1.0 
            uint64_t random_key = rand() % tuple_num; // 0 ~ tuple_num-1

            if (rand_value < read_ratio) {
                // Read
                transactions[i].task_set_.emplace_back(Ope::READ, random_key);
            } else {
                // Write
                transactions[i].task_set_.emplace_back(Ope::WRITE, random_key);
            }
        }
    }
}

#endif // COMMON_HPP
