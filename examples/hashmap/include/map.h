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
        std::size_t operator() (WriteId const &wid) const {
                        return (std::hash<uint64_t>{}(*(uint64_t*)&wid.bytes[0])*71) ^ std::hash<uint64_t>{}(*(uint64_t*)&wid.bytes[8]);
        }
};

class wid_equality {
public:
        bool operator() (WriteId const &wid1, WriteId const &wid2) const {
                uint64_t const *wid1_1 = (uint64_t const *)&wid1.bytes[0];
                uint64_t const *wid1_2 = (uint64_t const *)&wid1.bytes[8];
                uint64_t const *wid2_1 = (uint64_t const *)&wid2.bytes[0];
                uint64_t const *wid2_2 = (uint64_t const *)&wid2.bytes[8];
                return (wid1_1 == wid2_1) && (wid1_2 == wid2_2);
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
        FLPtr                                           m_fuzzylog_client;
        bool                                            m_replication;


public:
        BaseMap(std::vector<std::string>* log_addr, bool replication) {
                this->m_replication = replication;
                init_fuzzylog_client(log_addr);
        }
        //MapType get_map_type() { return m_map_type; }
        void init_fuzzylog_client(std::vector<std::string>* log_addr);
        // Synchronous operations
        void put(uint64_t key, uint64_t value, ColorSpec op_color);
        void remove(uint64_t key, ColorSpec op_color);
        // Asynchronous operations
        WriteId async_put(uint64_t key, uint64_t value, ColorSpec op_color);
        void flush_completed_puts();
        WriteId try_wait_for_any_put();
        WriteId wait_for_any_put();
        void wait_for_all_puts();
};
