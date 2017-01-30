#ifndef           HASHMAP_H_
#define           HASHMAP_H_

#include <stdint.h>
#include <unordered_map>

extern "C" {
        #include "fuzzy_log.h"
}

#define PARTITION_COUNT 4

class HashMap {
private:
        std::unordered_map<uint32_t, uint32_t> cache;  
        DAGHandle** fuzzylog_clients;
public:
        HashMap();
        ~HashMap();

        uint32_t get(uint32_t key);
        void put(uint32_t key, uint32_t value);
        void remove(uint32_t key);
};

#endif            // HASHMAP_H_
