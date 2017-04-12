#pragma once

#include <runnable.h>
#include <chrono>
#include <vector>

struct latency_footprint {
        std::chrono::system_clock::time_point           m_issue_time;
        uint64_t                                        m_latency;
};

class Tester: public Runnable {
protected:
        std::vector<latency_footprint>  m_put_latencies;
        std::vector<latency_footprint>  m_get_latencies;
public:
        Tester() {}
        virtual ~Tester() {}
        virtual void run() = 0;
        virtual void join() = 0;
        void add_put_latency(std::chrono::system_clock::time_point issue_time);
        void add_get_latency(std::chrono::system_clock::time_point issue_time);
        void get_put_latencies(std::vector<latency_footprint> &latencies);
        void get_get_latencies(std::vector<latency_footprint> &latencies);
};
