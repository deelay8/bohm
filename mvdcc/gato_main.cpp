#include "config.hpp"
#include "common.hpp"
#include "gato_cc.hpp"
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>

// 전역 변수 정의
std::vector<Tuple> Table;                                  // 데이터베이스 테이블
std::vector<Transaction> transactions;                    // 트랜잭션 리스트
std::priority_queue<Transaction, std::vector<Transaction>, 
                   std::function<bool(const Transaction&, const Transaction&)>> ready_queue(
    [](const Transaction& a, const Transaction& b) {
        return a.timestamp_ > b.timestamp_;               // 낮은 타임스탬프 우선순위
    }); 
uint64_t tx_counter = 0;                                  // 트랜잭션 카운터
std::vector<std::vector<uint64_t>> thread_partitions;     // 스레드별 레코드 파티션
std::vector<int> thread_load;                             // 각 스레드의 부하 상태
std::unordered_map<uint64_t, int> record_to_thread;       // 레코드 -> 스레드 매핑
std::unordered_map<uint64_t, int> last_writer;            // 마지막으로 쓰기 작업을 수행한 스레드
std::mutex mapping_mutex;                                 // 매핑 동기화 뮤텍스
std::mutex partition_mutex;                               // 파티션 동기화 뮤텍스
std::condition_variable ready_queue_cv;                  // Ready Queue 조건 변수

int main() {
    size_t thread_num = DEFAULT_THREAD_NUM;
    size_t tuple_num = DEFAULT_TUPLE_NUM;

    // 데이터베이스 및 트랜잭션 초기화
    makeDB(tuple_num);
    initializeTransactions(tuple_num);

    // 스레드별 부하 초기화
    thread_load.resize(thread_num, 0);

    // 레코드 분배 (Static Partitioning)
    assignRecordsToThreads(thread_num, tuple_num);

    // 디버깅: 초기 레코드 분배 상태 출력
    //debugThreadLoad();

    bool start = false;
    bool quit = false;

    std::vector<std::thread> cc_workers;

    // Gato CC 워커 실행
    for (size_t i = 0; i < thread_num; ++i) {
        cc_workers.emplace_back(gato_cc_worker, i, std::ref(start), std::ref(quit));
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
    std::cout << "Execution finished. Final thread load distribution:" << std::endl;
    debugThreadLoad();

    return 0;
}
