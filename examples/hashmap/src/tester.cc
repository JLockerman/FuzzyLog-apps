#include <tester.h>

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
