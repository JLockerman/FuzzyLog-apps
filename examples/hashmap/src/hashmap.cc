#include "hashmap.h"
#include <iostream>
#include <cassert>

//static const char *server_ip = "127.0.0.1:9990";
// FIXME: for experiment, use the below server
static const char *server_ip = "52.15.156.76:9990";

static char out[DELOS_MAX_DATA_SIZE];

HashMap::HashMap(vector<uint32_t> *color_of_interest) {
        uint32_t i;
        struct colors* color;
        
        // initialize color
        color = (struct colors*)malloc(sizeof(struct colors));
        color->numcolors = color_of_interest->size();
        color->mycolors  = new ColorID[color->numcolors];
        for (i = 0; i < color->numcolors; ++i) {
                color->mycolors[i] = color_of_interest->at(i);
        }
        this->color = color;

        // connect to Fuzzylog
        fzlog_client = new_dag_handle_for_single_server(server_ip, this->color);
}

HashMap::~HashMap() {
        close_dag_handle(fzlog_client);
}

uint32_t HashMap::get(uint32_t key, struct colors* op_color) {
        uint64_t data;
        uint32_t val;
        size_t size = 0;

        // acquire lock
        fzlog_lock.lock();

        // snapshot, get_next
        snapshot(fzlog_client);
        while ((get_next(fzlog_client, out, &size, op_color), 1)) {
                if (op_color->numcolors == 0) break;
                data = *(uint64_t *)out; 
                if ((uint32_t)(data >> 32) != key) continue;
                val = (uint32_t)(data & 0xFFFFFFFF); 
        }

        // check if no entry
        if (val == 0) {
                // FIXME: instead, throw exception?
                fzlog_lock.unlock();
                return 0;
        }

        // cache the value
        cache[key] = val;        

        // release lock 
        fzlog_lock.unlock();

        return val;
}


void HashMap::put(uint32_t key, uint32_t value, struct colors* op_color) {
        // acquire lock
        fzlog_lock.lock();

        // append
        uint64_t data = ((uint64_t)key << 32) | value;
        append(fzlog_client, (char *)&data, sizeof(data), op_color, NULL);

        // release lock
        fzlog_lock.unlock();
}

void HashMap::multiput(vector<uint32_t>* keys, vector<uint32_t>* values, vector<struct colors*>* op_colors) {
        assert(op_colors->size() == keys->size());
        assert(op_colors->size() == values->size());

        uint32_t i, key, value;

        // acquire lock
        fzlog_lock.lock();

        for (i = 0; i < keys->size(); ++i) {
                key = keys->at(i);
                value = values->at(i);
                // append
                uint64_t data = ((uint64_t)key << 32) | value;
                // get color
                append(fzlog_client, (char *)&data, sizeof(data), op_colors->at(i), NULL);
        }

        // release lock
        fzlog_lock.unlock();
}

void HashMap::remove(uint32_t key, struct colors* op_color) {
        // acquire lock
        fzlog_lock.lock();

        // append
        uint64_t data = (uint64_t)key << 32;
        append(fzlog_client, (char *)&data, sizeof(data), op_color, NULL);

        // remove cache
        cache.erase(key);

        // release lock
        fzlog_lock.unlock();
}
