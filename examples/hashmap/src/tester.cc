#include <tester.h>
#include <iostream>

uint64_t Tester::get_num_executed() {
        return m_context->get_num_executed();
}

uint64_t Tester::get_num_committed() {
        return m_context->get_num_committed();
}

uint32_t Tester::try_get_completed() {
        uint32_t num_completed = 0;
        while (true) {
                write_id wid = m_map->try_wait_for_any_put();
                if (wid_equality{}(wid, WRITE_ID_NIL)) 
                        break; 
                m_context->inc_num_executed();
                num_completed += 1;
                // Add latency stat
                auto start_time = m_context->mark_ended(wid); 
                add_put_latency(start_time);
        }
        return num_completed;
}

bool Tester::is_duration_based_run() {
        return m_expt_duration > 0;
}

bool Tester::is_all_executed() {
        return m_context->get_num_executed() == m_num_txns;
}

void Tester::reset_throttler() {
        m_started_at = std::chrono::system_clock::now();
        m_num_issued = 0;
}

bool Tester::is_throttled() {
        if (m_txn_rate == 0)
                return false;
        auto now = std::chrono::system_clock::now();
        std::chrono::duration<uint64_t, std::nano> elapsed = now - m_started_at;
        // rate 
        double txn_rate = m_num_issued * 1000000000L / elapsed.count(); 
        return m_txn_rate <= txn_rate;
}

void Tester::add_put_latency(std::chrono::system_clock::time_point issue_time) {
        latency_footprint lf;
        // issue time
        lf.m_issue_time = issue_time;
        // latency
        auto end_time = std::chrono::system_clock::now();
        std::chrono::duration<uint64_t, std::nano> elapsed = end_time - issue_time;
        lf.m_latency = elapsed.count();
        // add
        m_put_latencies.push_back(lf);
}

void Tester::add_get_latency(std::chrono::system_clock::time_point issue_time) {
        latency_footprint lf;
        // issue time
        lf.m_issue_time = issue_time;
        // latency
        auto end_time = std::chrono::system_clock::now();
        std::chrono::duration<uint64_t, std::nano> elapsed = end_time - issue_time;
        lf.m_latency = elapsed.count();
        // add
        m_get_latencies.push_back(lf);
}

void Tester::get_put_latencies(std::vector<latency_footprint> &latencies) {
        for (auto l : m_put_latencies) {
                latencies.push_back(l);
        }
}

void Tester::get_get_latencies(std::vector<latency_footprint> &latencies) {
        for (auto l : m_get_latencies) {
                latencies.push_back(l);
        }
}
