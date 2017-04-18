#pragma once

#include <runnable.h>
#include <chrono>
#include <vector>
#include <request.h>

struct latency_footprint {
        std::chrono::system_clock::time_point           m_issue_time;
        uint64_t                                        m_latency;
};

class Tester: public Runnable {
protected:
        Context*                        m_context;
        BaseMap*                        m_map;
        Txn**                           m_txns;
        uint64_t                        m_num_txns;
        bool                            m_async;
        uint32_t                        m_window_size;
        uint32_t                        m_expt_duration;
        uint32_t                        m_txn_rate;
        std::atomic<bool>*              m_flag;
        // Number of operations
        std::atomic<uint64_t>           m_num_issued; 
        std::atomic<uint64_t>           m_num_executed; 
        // Latencies 
        std::vector<latency_footprint>  m_put_latencies;
        std::vector<latency_footprint>  m_get_latencies;
        // Execution time 
        std::chrono::system_clock::time_point   m_started_at;

public:
        Tester(Context* context, BaseMap* map, std::atomic<bool>* flag, Txn** txns, uint64_t num_txns, bool async, uint32_t window_size, uint32_t expt_duration, uint32_t txn_rate) {
                this->m_context = context;
                this->m_map = map;
                this->m_flag = flag;
                this->m_txns = txns;
                this->m_num_txns = num_txns;
                this->m_async = async;
                this->m_window_size = window_size;
                this->m_expt_duration = expt_duration;
                this->m_txn_rate = txn_rate;
                this->m_num_issued = 0;
                this->m_num_executed = 0;
        }
        virtual ~Tester() {}
        virtual void run() = 0;
        virtual void join() = 0;
        uint64_t get_num_executed();
        uint64_t get_num_committed();
        uint32_t try_get_completed();
        bool is_duration_based_run();
        bool is_all_executed();
        void reset_throttler();
        bool is_throttled();
        void add_put_latency(std::chrono::system_clock::time_point issue_time);
        void add_get_latency(std::chrono::system_clock::time_point issue_time);
        void get_put_latencies(std::vector<latency_footprint> &latencies);
        void get_get_latencies(std::vector<latency_footprint> &latencies);
};
