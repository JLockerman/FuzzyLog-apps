#include "hashmap.h"
#include <iostream>
#include <cassert>

//static const char *server_ip = "127.0.0.1:9990";
// FIXME: for experiment, use the below server
static const char *server_ip = "52.15.156.76:9990";

static char out[DELOS_MAX_DATA_SIZE];

HashMap::HashMap(struct colors* color_single, struct colors* color_multi) {
        this->color_single = color_single;
        this->color_multi = color_multi;

        if (color_multi != NULL)
                fzlog_client = new_dag_handle_for_single_server(server_ip, color_multi);
        else
                fzlog_client = new_dag_handle_for_single_server(server_ip, color_single);
}

HashMap::~HashMap() {
        close_dag_handle(fzlog_client);
}

uint32_t HashMap::get(uint32_t key) {
        uint64_t data;
        uint32_t val;
        size_t size = 0;

        // acquire lock
        fzlog_lock.lock();

        // snapshot, get_next
        snapshot(fzlog_client);
        while ((get_next(fzlog_client, out, &size, color_single), 1)) {
                if (color_single->numcolors == 0) break;
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


void HashMap::put(uint32_t key, uint32_t value) {
        // acquire lock
        fzlog_lock.lock();

        // append
        uint64_t data = ((uint64_t)key << 32) | value;
        append(fzlog_client, (char *)&data, sizeof(data), color_single, NULL);

        // release lock
        fzlog_lock.unlock();
}

void HashMap::multiput(vector<uint32_t> keys, vector<uint32_t> values) {
        assert(color_multi != NULL);
        assert(color_multi->numcolors == keys.size());
        assert(color_multi->numcolors == values.size());

        uint32_t i, key, value;

        // acquire lock
        fzlog_lock.lock();

        for (i = 0; i < keys.size(); ++i) {
                key = keys[i];
                value = values[i];
                // append
                uint64_t data = ((uint64_t)key << 32) | value;
                // get color
                struct colors color;
                get_color_by_idx(i, &color); 
                append(fzlog_client, (char *)&data, sizeof(data), &color, NULL);
        }

        // release lock
        fzlog_lock.unlock();
}

void HashMap::get_color_by_idx(uint32_t idx, struct colors* out) {
        assert(color_multi != NULL);

        out->numcolors = 1;
        out->mycolors = new ColorID[0];
        out->mycolors[0] = color_multi->mycolors[idx];          
}

void HashMap::remove(uint32_t key) {
        // acquire lock
        fzlog_lock.lock();

        // append
        uint64_t data = (uint64_t)key << 32;
        append(fzlog_client, (char *)&data, sizeof(data), color_single, NULL);

        // remove cache
        cache.erase(key);

        // release lock
        fzlog_lock.unlock();
}
