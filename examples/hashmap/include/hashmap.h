#ifndef           HASHMAP_H_
#define           HASHMAP_H_

#include <stdint.h>
#include <unordered_map>
#include <mutex>
#include <vector>

extern "C" {
        #include "fuzzy_log.h"
}

class HashMap {
private:
        uint32_t partition_count;
        std::unordered_map<uint32_t, uint32_t> cache;  
        DAGHandle** fuzzylog_clients;
        std::vector<std::mutex*> locks;
        
public:
        HashMap(uint32_t partition_count);
        ~HashMap();

        uint32_t get(uint32_t key);
        void put(uint32_t key, uint32_t value);
        void remove(uint32_t key);
};

#endif            // HASHMAP_H_
