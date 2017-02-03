#ifndef           HASHMAP_H_
#define           HASHMAP_H_

#include <stdint.h>
#include <unordered_map>
#include <mutex>
#include <vector>

extern "C" {
        #include "fuzzy_log.h"
}

using namespace std;

class HashMap {
private:
        struct colors* color_single;
        struct colors* color_multi;
        unordered_map<uint32_t, uint32_t> cache;  
        DAGHandle* fzlog_client;
        mutex fzlog_lock;
        
public:
        HashMap(struct colors* color_single, struct colors* color_multi);
        ~HashMap();

        uint32_t get(uint32_t key);
        void put(uint32_t key, uint32_t value);
        void multiput(vector<uint32_t> keys, vector<uint32_t> values);
        void remove(uint32_t key);

        // helper
        void get_color_by_idx(uint32_t idx, struct colors* out);
};

#endif            // HASHMAP_H_
