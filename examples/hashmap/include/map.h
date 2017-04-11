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

class wid_hasher {
public:
        std::size_t operator() (write_id const &wid) const {
                        return (std::hash<uint64_t>{}(wid.p1)*71) ^ std::hash<uint64_t>{}(wid.p2);              
        }
};

class wid_equality {
public:
        bool operator() (write_id const &wid1, write_id const &wid2) const {
                return (wid1.p1 == wid2.p1) && (wid1.p2 == wid2.p2);
        }
};

class BaseMap {
public:
        enum MapType {
                atomic_map = 1,
                cap_map = 2,
        };

protected:
        // Fuzzylog connection
        DAGHandle*                                      m_fuzzylog_client;
        bool                                            m_replication;

public:
        BaseMap(std::vector<std::string>* log_addr, bool replication) {
                this->m_replication = replication;
                init_fuzzylog_client(log_addr);
        }
        //MapType get_map_type() { return m_map_type; }
        void init_fuzzylog_client(std::vector<std::string>* log_addr);
        // Synchronous operations
        void put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color);
        void remove(uint32_t key, struct colors* op_color);
        // Asynchronous operations
        write_id async_put(uint32_t key, uint32_t value, struct colors* op_color);
        void flush_completed_puts();
        write_id try_wait_for_any_put();
        write_id wait_for_any_put();
        void wait_for_all_puts();
};
