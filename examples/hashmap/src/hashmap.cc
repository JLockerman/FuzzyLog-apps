#include <hashmap.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unistd.h>

static char out[DELOS_MAX_DATA_SIZE];

HashMap::HashMap(std::vector<std::string>* log_addr) {
        init_fuzzylog_client(log_addr);        
}

void HashMap::init_fuzzylog_client(std::vector<std::string>* log_addr) {
        // FIXME: using temp snapshot color 
        struct colors color; 
        color.numcolors = 1;
        color.mycolors = (ColorID*)malloc(sizeof(ColorID));
        color.mycolors[0] = 0;

        // Initialize fuzzylog connection
        if (log_addr->size() == 1) {
                const char *server_ip = log_addr->at(0).c_str();
                m_fuzzylog_client = new_dag_handle_for_single_server(server_ip, &color);
        } else {
                const char *lock_server_ip = log_addr->at(0).c_str();
                size_t num_chain_servers = log_addr->size() - 1;
                const char *chain_server_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_ips[i] = log_addr->at(i+1).c_str();
                }
                m_fuzzylog_client = new_dag_handle(lock_server_ip, num_chain_servers, chain_server_ips, &color);
        }
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

        // Take snapshot and get_next
        snapshot(m_fuzzylog_client);
        while ((get_next(m_fuzzylog_client, out, &size, op_color), 1)) {
                if (op_color->numcolors == 0) break;
                data = *(uint64_t *)out;
                if ((uint32_t)(data >> 32) != key) continue;
                val = (uint32_t)(data & 0xFFFFFFFF); 
        }

        // Check if there is no entry
        if (val == 0) {
                // FIXME: instead, throw exception?
                return 0;
        }

        // Cache value
        m_cache[key] = val;        

        return val;
}

void HashMap::put(uint32_t key, uint32_t value, struct colors* op_color) {
        uint64_t data = ((uint64_t)key << 32) | value;
        // For latency measurement 
        auto start_time = std::chrono::system_clock::now();
        append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);
        auto end_time = std::chrono::system_clock::now();
        auto latency = end_time - start_time; 
        m_latencies.push_back(latency);
}

void HashMap::remove(uint32_t key, struct colors* op_color) {
        // Note: remove is the same as putting a zero value
        uint64_t data = (uint64_t)key << 32;
        append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);

        // Remove cache
        m_cache.erase(key);
}

void HashMap::async_put(uint32_t key, uint32_t value, struct colors* op_color) {
        // For latency measurement 
        auto start_time = std::chrono::system_clock::now();
        // Async append 
        uint64_t data = ((uint64_t)key << 32) | value;
        write_id wid = async_append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);
        new_write_id nwid;
        nwid.id = wid;
        // Mark start time
        m_start_time_map[nwid] = start_time;
}

void HashMap::wait_for_any_put() {
        // Wait for any append to completed
        write_id wid = wait_for_any_append(m_fuzzylog_client);
        // TODO: handle NIL (returned if there is no pending append)
        new_write_id nwid;
        nwid.id = wid; 

        // Measure latency
        auto searched = m_start_time_map.find(nwid);
        assert(searched != m_start_time_map.end());
        auto end_time = std::chrono::system_clock::now();
        auto latency = end_time - searched->second;
        m_latencies.push_back(latency);
        m_start_time_map.erase(nwid);
}

void HashMap::wait_for_all() {
        while (true) {
                if (m_start_time_map.size() == 0) break;
                wait_for_any_put();
        }
}

void HashMap::write_output_for_latency(const char* filename) {
        std::ofstream result_file; 
        result_file.open(filename, std::ios::out);
        for (auto l : m_latencies) {
                result_file << l.count() << "\n"; 
        }
        result_file.close();        
}
