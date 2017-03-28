#pragma once

#include <stdint.h>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <city.h>
#include <config.h>
#include <synchronizer.h>

// Extended version of the original write_id to be used as key in std::unordered_map
typedef struct new_write_id {
        write_id id;

        bool operator==(const new_write_id &other) const {
                return other.id.p1 == this->id.p1 &&
                other.id.p2 == this->id.p2;
        }

        bool operator!=(const new_write_id &other) const {
                return other.id.p1 != this->id.p1 ||
                other.id.p2 != this->id.p2;
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
        DAGHandle*                                      m_fuzzylog_client;
       
        // Color synchronizer
        Synchronizer*                                   m_synchronizer; 

public:
        HashMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload);
        ~HashMap();

        bool get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors);
        void init_fuzzylog_client(std::vector<std::string>* log_addr);
        void init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors);

        // Synchronous operations
        uint32_t get(uint32_t key);
        void put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color);
        void remove(uint32_t key, struct colors* op_color);

        // Asynchronous operations
        void async_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color);
        void flush_completed_puts();
        new_write_id try_wait_for_any_put();
        new_write_id wait_for_any_put();
        void wait_for_all_puts();
};
