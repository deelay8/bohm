#ifndef GATO_CC_HPP
#define GATO_CC_HPP

#include "common.hpp"
#include <unordered_map>
#include <vector>
#include <atomic>
#include <mutex>
#include <iostream>
#include <algorithm>

// 전역 변수 선언 (extern 키워드 사용)
extern std::vector<Result> AllResult; // 스레드별 성능 결과 저장
extern std::unordered_map<uint64_t, int> record_to_thread; // 레코드 -> 스레드 매핑
extern std::vector<int> thread_load;                       // 각 스레드의 부하
extern std::unordered_map<uint64_t, int> last_writer;      // 레코드의 마지막 쓰기 스레드
extern std::mutex mapping_mutex;                           // 매핑 테이블 동기화

// 초기 레코드 분배 (Static Partitioning)
void assignRecordsToThreads(size_t cc_thread_num, size_t tuple_num) {
    thread_partitions.resize(cc_thread_num);
    record_to_thread.clear(); // record_to_thread 초기화

    for (uint64_t i = 0; i < tuple_num; ++i) {
        size_t thread_id = i % cc_thread_num;
        thread_partitions[thread_id].push_back(i);
        record_to_thread[i] = thread_id; // 매핑 테이블 초기화
    }

    // 디버깅: 초기화 상태 확인
    //for (const auto& [key, thread] : record_to_thread) {
    //    std::cout << "[DEBUG] Record " << key << " assigned to Thread " << thread << std::endl;
    //}
}

// 디버깅: 스레드별 부하 상태 출력
void debugThreadLoad() {
    std::lock_guard<std::mutex> lock(mapping_mutex);
    for (size_t i = 0; i < thread_load.size(); ++i) {
        std::cout << "[DEBUG] Thread " << i << " load: " << thread_load[i] << std::endl;
    }
}

// 부하 재분배 함수
void redistributeLoad(int overloaded_thread, int underloaded_thread) {
    std::lock_guard<std::mutex> lock(mapping_mutex);

    // 유효한 스레드 확인
    if (overloaded_thread < 0 || overloaded_thread >= thread_load.size() ||
        underloaded_thread < 0 || underloaded_thread >= thread_load.size()) {
        std::cerr << "[ERROR] Invalid thread IDs in redistributeLoad." << std::endl;
        return;
    }

    for (auto& [record, thread_id] : record_to_thread) {
        if (thread_id == overloaded_thread) {
            record_to_thread[record] = underloaded_thread;
            thread_load[overloaded_thread]--;
            thread_load[underloaded_thread]++;
            std::cout << "[DEBUG] Moved Record " << record << " from Thread "
                      << overloaded_thread << " to Thread " << underloaded_thread << std::endl;
            return; // 한 번에 하나의 레코드만 이동
        }
    }
}

void gato_cc_worker(int thread_id, const bool& start, const bool& quit) {
    size_t iteration_count = 0; // 디버깅 빈도를 제어하기 위한 카운터

    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST)) {}

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST)) {
        std::vector<Transaction> local_batch;

        // 트랜잭션 배치 가져오기
        uint64_t start_pos = __atomic_fetch_add(&tx_counter, BATCH_SIZE, __ATOMIC_SEQ_CST);
        uint64_t end_pos = std::min(start_pos + BATCH_SIZE, (uint64_t)transactions.size());
        for (uint64_t i = start_pos; i < end_pos; ++i) {
            local_batch.emplace_back(transactions[i]);
        }

        // CC 단계 실행
        for (auto& trans : local_batch) {
            bool is_success = true;

            for (const auto& task : trans.task_set_) {
                {
                    std::lock_guard<std::mutex> lock(mapping_mutex);
                    if (record_to_thread.find(task.key_) == record_to_thread.end()) {
                        is_success = false; // 관리되지 않은 레코드가 있으면 실패 처리
                        continue;
                    }
                    int assigned_thread = record_to_thread[task.key_];
                    if (assigned_thread != thread_id) continue; // 현재 스레드가 관리하지 않으면 스킵
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

            // 트랜잭션 처리 성공 시 결과 업데이트
            if (is_success) {
                std::lock_guard<std::mutex> result_lock(mapping_mutex);
                AllResult[thread_id].commit_cnt_++; // 커밋된 트랜잭션 카운트 증가
            }

            // 부하 증가
            thread_load[thread_id]++;
        }
        iteration_count++;

        // 부하 감지 및 재분배
        {
            int max_load = *std::max_element(thread_load.begin(), thread_load.end());
            int min_load = *std::min_element(thread_load.begin(), thread_load.end());
            if (max_load - min_load > 10) { // 부하 차이가 임계값 초과
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
