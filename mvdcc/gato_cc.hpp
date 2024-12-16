#ifndef GATO_CC_HPP
#define GATO_CC_HPP

#include "common.hpp"
#include <unordered_map>
#include <vector>
#include <atomic>
#include <mutex>
#include <iostream>
#include <algorithm>

// Global Variable Declaration (Using the extern Keyword)
extern std::vector<Result> AllResult; // Store Performance Results per Thread
extern std::unordered_map<uint64_t, int> record_to_thread; // Record-to-Thread Mapping
extern std::vector<int> thread_load;                       // Load of Each Thread
extern std::unordered_map<uint64_t, int> last_writer;      // Last Writing Thread of a Record
extern std::mutex mapping_mutex;                           // Synchronize Mapping Table

// Static Partitioning
void assignRecordsToThreads(size_t cc_thread_num, size_t tuple_num) {
    thread_partitions.resize(cc_thread_num);
    record_to_thread.clear(); // Initialize record_to_thread

    for (uint64_t i = 0; i < tuple_num; ++i) {
        size_t thread_id = i % cc_thread_num;
        thread_partitions[thread_id].push_back(i);
        record_to_thread[i] = thread_id; // Initialize Mapping Table
    }

    // Debug: Check Initialization Status
    //for (const auto& [key, thread] : record_to_thread) {
    //    std::cout << "[DEBUG] Record " << key << " assigned to Thread " << thread << std::endl;
    //}
}

// Debug: Print Load Status per Thread
// void debugThreadLoad() {
//     std::lock_guard<std::mutex> lock(mapping_mutex);
//     for (size_t i = 0; i < thread_load.size(); ++i) {
//         std::cout << "[DEBUG] Thread " << i << " load: " << thread_load[i] << std::endl;
//     }
// }

// Load Redistribution Function
void redistributeLoad(int overloaded_thread, int underloaded_thread) {
    std::lock_guard<std::mutex> lock(mapping_mutex);

    // Validate Threads
    if (overloaded_thread < 0 || overloaded_thread >= thread_load.size() ||
        underloaded_thread < 0 || underloaded_thread >= thread_load.size()) {
        // std::cerr << "[ERROR] Invalid thread IDs in redistributeLoad." << std::endl;
        return;
    }

    for (auto& [record, thread_id] : record_to_thread) {
        if (thread_id == overloaded_thread) {
            record_to_thread[record] = underloaded_thread;
            thread_load[overloaded_thread]--;
            thread_load[underloaded_thread]++;
            // std::cout << "[DEBUG] Moved Record " << record << " from Thread "
            //           << overloaded_thread << " to Thread " << underloaded_thread << std::endl;
            return; // Move Only One Record at a Time
        }
    }
}

void gato_cc_worker(int thread_id, const bool& start, const bool& quit) {
    size_t iteration_count = 0; // Counter to Control Debugging Frequency

    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        std::vector<Transaction> local_batch;

        // Fetch Transaction Batch
        uint64_t start_pos = __atomic_fetch_add(&tx_counter, BATCH_SIZE, __ATOMIC_SEQ_CST);
        uint64_t end_pos = std::min(start_pos + BATCH_SIZE, (uint64_t)transactions.size());
        for (uint64_t i = start_pos; i < end_pos; ++i) {
            local_batch.emplace_back(transactions[i]);
        }

        // Execute CC phase
        for (auto& trans : local_batch) {
            bool is_success = true;

            for (const auto& task : trans.task_set_) {
                {
                    std::lock_guard<std::mutex> lock(mapping_mutex);
                    if (record_to_thread.find(task.key_) == record_to_thread.end()) {
                        is_success = false; // Handle Failure if Unmanaged Records Exist
                        continue;
                    }
                    int assigned_thread = record_to_thread[task.key_];
                    if (assigned_thread != thread_id) continue; // Skip if Not Managed by the Current Thread
                }

                if (task.ope_ == Ope::WRITE) {
                    {
                        std::lock_guard<std::mutex> lock(mapping_mutex);
                        if (last_writer[task.key_] != thread_id) {
                            last_writer[task.key_] = thread_id;
                        }
                    }
                    Table[task.key_].addPlaceholder(trans.timestamp_);
                    trans.write_set_.emplace_back(task.key_);
                }
            }

            // Update Results Upon Successful Transaction Processing
            if (is_success) {
                std::lock_guard<std::mutex> result_lock(mapping_mutex);
                AllResult[thread_id].commit_cnt_++; // Increment Committed Transaction Count
            }

            // Increase Load
            thread_load[thread_id]++;
        }
        iteration_count++;

        // Detect and Redistribute Load
        {
            int max_load = *std::max_element(thread_load.begin(), thread_load.end());
            int min_load = *std::min_element(thread_load.begin(), thread_load.end());
            if (max_load - min_load > BATCH_SIZE/DEFAULT_THREAD_NUM) { // Load Difference Exceeds Threshold
                int overloaded_thread = std::distance(thread_load.begin(),
                                        std::max_element(thread_load.begin(), thread_load.end()));
                int underloaded_thread = std::distance(thread_load.begin(),
                                          std::min_element(thread_load.begin(), thread_load.end()));
                redistributeLoad(overloaded_thread, underloaded_thread);
            }
        }
    }
}


#endif // GATO_CC_HPP
