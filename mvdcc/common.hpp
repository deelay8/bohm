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

// Result 클래스: 스레드별 트랜잭션 결과 저장
class Result {
public:
    uint64_t commit_cnt_ = 0; // 커밋된 트랜잭션 수
};

// Task 클래스: 트랜잭션 작업 정의
enum class Ope { READ, WRITE };

class Task {
public:
    Ope ope_;
    uint64_t key_;

    Task(Ope ope, uint64_t key) : ope_(ope), key_(key) {}
};

// Transaction 클래스: 트랜잭션 데이터
class Transaction {
public:
    uint64_t timestamp_;
    std::vector<std::pair<uint64_t, uint64_t>> read_set_;
    std::vector<uint64_t> write_set_;
    std::vector<Task> task_set_; // Task 작업 리스트

    Transaction(uint64_t timestamp) : timestamp_(timestamp) {}
    Transaction() : timestamp_(0) {}
};

// Tuple 클래스: 데이터베이스의 레코드
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

// 전역 변수 정의
extern std::vector<Tuple> Table;
extern std::vector<Transaction> transactions;
extern std::priority_queue<Transaction, std::vector<Transaction>, 
                           std::function<bool(const Transaction&, const Transaction&)>> ready_queue;
extern uint64_t tx_counter;
extern std::vector<std::vector<uint64_t>> thread_partitions;
extern std::mutex partition_mutex;
extern std::condition_variable ready_queue_cv;

// 공통 함수 정의
void makeDB(size_t tuple_num) {
    Table.resize(tuple_num);
    for (size_t i = 0; i < tuple_num; ++i) {
        Table[i] = Tuple();
    }
}

void initializeTransactions(size_t tuple_num) {
    transactions.resize(tuple_num);
    for (uint64_t i = 0; i < tuple_num; ++i) {
        transactions[i] = Transaction(i);
        transactions[i].task_set_ = {
            Task(Ope::READ, i % tuple_num),
            Task(Ope::WRITE, (i + 1) % tuple_num)
        };
    }
}

#endif // COMMON_HPP
