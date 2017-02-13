#include "hashmap.h"
#include <iostream>
#include <cassert>
#include <unistd.h>

//static const char *server_ip = "127.0.0.1:9990";
// FIXME: for experiment, use the below server
static const char *server_ip = "52.15.156.76:9990";

static char out[DELOS_MAX_DATA_SIZE];

mutex HashMap::m_promises_mtx;

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
        this->m_color = color;

        // connect to Fuzzylog
        m_fuzzylog_client = new_dag_handle_for_single_server(server_ip, this->m_color);
}

HashMap::~HashMap() {
        close_dag_handle(m_fuzzylog_client);
}

uint32_t HashMap::get(uint32_t key, struct colors* op_color) {
        uint64_t data;
        uint32_t val;
        size_t size;

        val = 0;
        size = 0;

        // acquire lock
        m_fuzzylog_mutex.lock();

        // snapshot, get_next
        snapshot(m_fuzzylog_client);
        while ((get_next(m_fuzzylog_client, out, &size, op_color), 1)) {
                if (op_color->numcolors == 0) break;
                data = *(uint64_t *)out; 
                if ((uint32_t)(data >> 32) != key) continue;
                val = (uint32_t)(data & 0xFFFFFFFF); 
        }

        // check if no entry
        if (val == 0) {
                // FIXME: instead, throw exception?
                m_fuzzylog_mutex.unlock();
                return 0;
        }

        // cache the value
        m_cache[key] = val;        

        // release lock 
        m_fuzzylog_mutex.unlock();

        return val;
}

void HashMap::put(uint32_t key, uint32_t value, struct colors* op_color) {
        // acquire lock
        m_fuzzylog_mutex.lock();

        // append
        uint64_t data = ((uint64_t)key << 32) | value;
        append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);

        // release lock
        m_fuzzylog_mutex.unlock();
}

void HashMap::remove(uint32_t key, struct colors* op_color) {
        // acquire lock
        m_fuzzylog_mutex.lock();

        // append
        uint64_t data = (uint64_t)key << 32;
        append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);

        // remove cache
        m_cache.erase(key);

        // release lock
        m_fuzzylog_mutex.unlock();
}

void HashMap::async_put(uint32_t key, uint32_t value, struct colors* op_color) {
        // acquire lock
        m_fuzzylog_mutex.lock();

        // append
        uint64_t data = ((uint64_t)key << 32) | value;
        write_id wid = async_append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);
        // release lock
        m_fuzzylog_mutex.unlock();

        // insert
        new_write_id nwid;
        nwid.id = wid;

        m_promises_mtx.lock();
        m_promises[nwid] = 0;
        m_promises_mtx.unlock();
}

void HashMap::wait_all() {
        while (true) {
                if (m_promises.size() == 0) return;

                write_id wid = wait_for_any_append(m_fuzzylog_client);
                new_write_id nwid;
                nwid.id = wid; 

                if (m_promises.find(nwid) != m_promises.end())
                        m_promises.erase(nwid);
        }
}
