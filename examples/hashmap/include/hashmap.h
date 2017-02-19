#ifndef           HASHMAP_H_
#define           HASHMAP_H_

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

using namespace std;

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
        struct colors*                          m_color;
        unordered_map<uint32_t, uint32_t>       m_cache;  
        DAGHandle*                              m_fuzzylog_client;
        mutex                                   m_fuzzylog_mutex;
       
        // Map for tracking latencies
        unordered_map<new_write_id, chrono::time_point<chrono::system_clock>>   m_start_time_map;
        static mutex                                                            m_start_time_map_mtx;
        vector<std::chrono::duration<double>>                                   m_latencies;
public:
        HashMap(vector<uint32_t>* color_of_interest);
        ~HashMap();

        // Synchronous operations
        uint32_t get(uint32_t key, struct colors* op_color);
        void put(uint32_t key, uint32_t value, struct colors* op_color);
        void remove(uint32_t key, struct colors* op_color);

        // Asynchronous operations
        void async_put(uint32_t key, uint32_t value, struct colors* op_color);
        void wait_for_any_put();
        void wait_for_all();
        void write_output_for_latency();
};

#endif            // HASHMAP_H_
