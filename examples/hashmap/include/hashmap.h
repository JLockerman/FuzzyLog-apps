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
        struct colors* color;
        unordered_map<uint32_t, uint32_t> cache;  
        DAGHandle* fzlog_client;
        mutex fzlog_lock;
        
public:
        HashMap(vector<uint32_t>* color_of_interest);
        ~HashMap();

        uint32_t get(uint32_t key, struct colors* op_color);
        void put(uint32_t key, uint32_t value, struct colors* op_color);
        void multiput(vector<uint32_t>* keys, vector<uint32_t>* values, vector<struct colors*>* op_colors);
        void remove(uint32_t key, struct colors* op_color);
};

#endif            // HASHMAP_H_
