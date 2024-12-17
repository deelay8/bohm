#include "config.hpp"
#include "common.hpp"
#include "bohm_cc.hpp"
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>

// Initialize Global Variables
std::vector<Tuple> Table;
std::vector<Transaction> transactions;
std::priority_queue<Transaction, std::vector<Transaction>, 
                   std::function<bool(const Transaction&, const Transaction&)>> ready_queue(
    [](const Transaction& a, const Transaction& b) {
        return a.timestamp_ > b.timestamp_; // Low Timestamp Priority
    });
uint64_t tx_counter = 0;
std::vector<std::vector<uint64_t>> thread_partitions;
std::mutex partition_mutex;
std::condition_variable ready_queue_cv;
std::vector<Result> AllResult; // Store Performance Results Per Thread

int main() {
    size_t thread_num = DEFAULT_THREAD_NUM; 
    size_t tuple_num = DEFAULT_TUPLE_NUM;
    double read_ratio = 0.5; 

    // Initialize Database and Transactions
    makeDB(tuple_num);
    initializeTransactions(tuple_num, read_ratio);

    // Initialize Results Per Thread
    AllResult.resize(thread_num);

    // Allocate Records to CC Threads
    assignRecordsToCCThreads(thread_num, tuple_num);

    bool start = false;
    bool quit = false;

    std::vector<std::thread> cc_workers;

    // Measure Start Time
    auto start_time = std::chrono::high_resolution_clock::now();

    // Execute CC Worker
    for (size_t i = 0; i < thread_num; ++i) {
        cc_workers.emplace_back(cc_worker, i, std::ref(start), std::ref(quit));
    }

    // Start Worker
    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);

    // Wait Until All Transactions Are Processed
    while (tx_counter < transactions.size()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
    }

    // Send End Signal
    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);

    // Wait for All Worker Threads to Finish
    for (auto& worker : cc_workers) {
        worker.join();
    }

    // Measure End Time
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    // Calculate Throughput
    uint64_t total_commits = 0;
    for (const auto& result : AllResult) {
        total_commits += result.commit_cnt_;
    }

    double throughput = total_commits / elapsed.count(); // Calculate Throughput (txn/sec)

    // Print Results
    std::cout << "Bohm Algorithm Performance:" << std::endl;
    std::cout << "Execution Time: " << elapsed.count() << " seconds" << std::endl;
    std::cout << "Total Transactions Committed: " << total_commits << std::endl;
    std::cout << "Throughput: " << throughput << " transactions/sec" << std::endl;

    return 0;
}
