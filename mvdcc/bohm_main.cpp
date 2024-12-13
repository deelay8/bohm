#include "config.hpp"
#include "common.hpp"
#include "bohm_cc.hpp"
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>

// 전역 변수 초기화
std::vector<Tuple> Table;
std::vector<Transaction> transactions;
std::priority_queue<Transaction, std::vector<Transaction>, 
                   std::function<bool(const Transaction&, const Transaction&)>> ready_queue(
    [](const Transaction& a, const Transaction& b) {
        return a.timestamp_ > b.timestamp_; // 낮은 타임스탬프 우선순위
    });
uint64_t tx_counter = 0;
std::vector<std::vector<uint64_t>> thread_partitions;
std::mutex partition_mutex;
std::condition_variable ready_queue_cv;

int main() {
    size_t thread_num = DEFAULT_THREAD_NUM;
    size_t tuple_num = DEFAULT_TUPLE_NUM;

    // 데이터베이스 및 트랜잭션 초기화
    makeDB(tuple_num);
    initializeTransactions(tuple_num);

    // CC 스레드와 실행 스레드 분리
    size_t cc_thread_num = thread_num;
    size_t exec_thread_num = thread_num - cc_thread_num;

    // CC 스레드에 레코드 할당
    assignRecordsToCCThreads(cc_thread_num, tuple_num);

    // 레코드 분배 디버깅
    debugRecordDistribution(cc_thread_num);

    bool start = false;
    bool quit = false;

    std::vector<std::thread> cc_workers;

    // CC 워커 실행
    for (size_t i = 0; i < cc_thread_num; ++i) {
        cc_workers.emplace_back(cc_worker, i, std::ref(start), std::ref(quit));
    }

    // 워커 시작
    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);

    // 일정 시간 실행
    std::this_thread::sleep_for(std::chrono::seconds(EX_TIME));

    // 종료 신호 전송
    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);

    // 워커 종료 대기
    for (auto& worker : cc_workers) {
        worker.join();
    }

    // 결과 출력
    std::cout << "Execution finished." << std::endl;

    return 0;
}
