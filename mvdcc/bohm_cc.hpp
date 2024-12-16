#ifndef BOHM_CC_HPP
#define BOHM_CC_HPP

#include "common.hpp"
#include <vector>
#include <queue>
#include <algorithm>
#include <iostream>
#include <condition_variable>
#include <mutex>

// Global Data Definition
extern std::vector<Result> AllResult; // Store Performance Results per Thread
extern std::vector<std::vector<uint64_t>> thread_partitions;

// Record Distribution Function
void assignRecordsToCCThreads(size_t cc_thread_num, size_t tuple_num) {
    thread_partitions.resize(cc_thread_num);
    for (uint64_t i = 0; i < tuple_num; ++i) {
        size_t thread_id = i % cc_thread_num;
        thread_partitions[thread_id].push_back(i);
    }
}

void cc_worker(int thread_id, const bool& start, const bool& quit) {
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        std::vector<Transaction> local_batch;

        // Retrieve Transaction Batch
        uint64_t start_pos = __atomic_fetch_add(&tx_counter, BATCH_SIZE, __ATOMIC_SEQ_CST);
        uint64_t end_pos = std::min(start_pos + BATCH_SIZE, (uint64_t)transactions.size());
        for (uint64_t i = start_pos; i < end_pos; ++i) {
            local_batch.emplace_back(transactions[i]);
        }

        // Transaction Sorting
        std::sort(local_batch.begin(), local_batch.end(), [](const Transaction& a, const Transaction& b) {
            return a.timestamp_ < b.timestamp_;
        });

        // Execute CC Phase
        for (auto& trans : local_batch) {
            bool is_success = true;

            for (const auto& task : trans.task_set_) {
                // Process if the Record is Managed by the Current Thread
                if (task.ope_ == Ope::WRITE &&
                    std::find(thread_partitions[thread_id].begin(),
                              thread_partitions[thread_id].end(),
                              task.key_) != thread_partitions[thread_id].end()) {
                    Table[task.key_].addPlaceholder(trans.timestamp_);
                    trans.write_set_.emplace_back(task.key_);
                }
            }

            // Add to Ready Queue
            {
                std::lock_guard<std::mutex> lock(partition_mutex);
                ready_queue.push(trans);
            }

            // Update Results if the Transaction is Successfully Processed
            if (is_success) {
                AllResult[thread_id].commit_cnt_++;  // Increment the Number of Committed Transactions
            }
        }

        // Notify the Execution Worker
        ready_queue_cv.notify_all();
    }
}


// Function to Debug Record Distribution
// void debugRecordDistribution(size_t cc_thread_num) {
    // for (size_t thread_id = 0; thread_id < cc_thread_num; ++thread_id) {
        // std::cout << "[DEBUG] CC Thread " << thread_id << " assigned records: ";
        // for (const auto& record : thread_partitions[thread_id]) {
        //     std::cout << record << " ";
        // }
        // std::cout << std::endl;
    // }
// }

#endif // BOHM_CC_HPP
