#pragma once

#include <stdint.h>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <city.h>

extern "C" {
        #include "fuzzy_log.h"
}


// Extended version of the original write_id to be used as key in std::unordered_map
typedef struct new_write_id {
        write_id id;

        bool operator==(const new_write_id &other) const {
                return other.id.p1 == this->id.p1 &&
                other.id.p2 == this->id.p2;
        }

} new_write_id;

static new_write_id NEW_WRITE_ID_NIL = {.id = WRITE_ID_NIL};

namespace std {
        template<>
                struct hash<new_write_id>
                {
                        std::size_t operator()(const new_write_id& k) const
                        {
                                return Hash128to64(make_pair(k.id.p1, k.id.p2));
                        }
                };
};


class HashMap {

private:
        std::unordered_map<uint32_t, uint32_t>          m_cache;  
        DAGHandle*                                      m_fuzzylog_client;
       
        // Map for tracking latencies
        std::unordered_map<new_write_id, std::chrono::time_point<std::chrono::system_clock>>   m_start_time_map;
        std::vector<std::chrono::duration<double>>                                             m_latencies;

public:
        HashMap(std::vector<std::string>* log_addr);
        ~HashMap();

        void init_fuzzylog_client(std::vector<std::string>* log_addr);

        // Synchronous operations
        uint32_t get(uint32_t key, struct colors* op_color);
        void put(uint32_t key, uint32_t value, struct colors* op_color);
        void remove(uint32_t key, struct colors* op_color);

        // Asynchronous operations
        void async_put(uint32_t key, uint32_t value, struct colors* op_color);
        void flush_completed_puts();
        new_write_id wait_for_any_put();
        void wait_for_all_puts();
};
