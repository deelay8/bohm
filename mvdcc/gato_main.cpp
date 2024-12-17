#include "config.hpp"
#include "common.hpp"
#include "gato_cc.hpp"
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>

// Define Global Variables
std::vector<Tuple> Table;                                  // Database Table
std::vector<Transaction> transactions;                    // Transaction List
std::priority_queue<Transaction, std::vector<Transaction>, 
                   std::function<bool(const Transaction&, const Transaction&)>> ready_queue(
    [](const Transaction& a, const Transaction& b) {
        return a.timestamp_ > b.timestamp_;               // Low Timestamp Priority
    }); 
uint64_t tx_counter = 0;                                  // Transaction Counter
std::vector<std::vector<uint64_t>> thread_partitions;     // Record Partition
std::vector<int> thread_load;                             // Load Status of Each Thread
std::unordered_map<uint64_t, int> record_to_thread;       // Record-to-Thread Mapping
std::unordered_map<uint64_t, int> last_writer;            // Last Write Thread
std::mutex mapping_mutex;                                 // Mapping Synchronization Mutex
std::mutex partition_mutex;                               // Partition Synchronization Mutex
std::condition_variable ready_queue_cv;                  // Ready Queue Condition Variable
std::vector<Result> AllResult;                           // Store Performance Results

int main() {
    size_t thread_num = DEFAULT_THREAD_NUM;
    size_t tuple_num = DEFAULT_TUPLE_NUM;
    double read_ratio = 0.5;

    // Initialize Database and Transactions
    makeDB(tuple_num);
    initializeTransactions(tuple_num, read_ratio);

    // Store Performance Results Per Thread
    thread_load.resize(thread_num, 0);
    AllResult.resize(thread_num);

    // Assign Records to Threads (Static Partitioning)
    assignRecordsToThreads(thread_num, tuple_num);

    // Gato Algorithm Start
    bool start = false;
    bool quit = false;

    std::vector<std::thread> cc_workers;

    // Measure Start Time
    auto start_time = std::chrono::high_resolution_clock::now();

    // Launch Gato CC Workers
    for (size_t i = 0; i < thread_num; ++i) {
        cc_workers.emplace_back(gato_cc_worker, i, std::ref(start), std::ref(quit));
    }

    // Start Workers
    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);

    // Wait Until All Transactions Are Processed
    while (tx_counter < transactions.size()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
    }

    // Send End Signal
    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);

    // Wait for Workers to Finish
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

    double throughput = total_commits / elapsed.count(); // Transactions per Second

    // Print Results
    std::cout << "Gato Algorithm Performance:" << std::endl;
    std::cout << "Execution Time: " << elapsed.count() << " seconds" << std::endl;
    std::cout << "Total Transactions Committed: " << total_commits << std::endl;
    std::cout << "Throughput: " << throughput << " transactions/sec" << std::endl;

    return 0;
}
