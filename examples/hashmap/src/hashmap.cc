#include <hashmap.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unistd.h>

#define DUMMY_INTERESTING_COLOR         100000

HashMap::HashMap(std::vector<std::string>* log_addr, uint8_t txn_version, std::vector<workload_config>* workload) {
        // find interesting colors from get workload
        init_fuzzylog_client(log_addr, txn_version); 

        std::vector<ColorID> interesting_colors;
        get_interesting_colors(workload, interesting_colors); 
        init_synchronizer(log_addr, txn_version, interesting_colors); 
}

void HashMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors) {
        uint32_t i;
        for (auto w : *workload) {
                if (w.op_type == "get") {
                        for (i = 0; i < w.color.numcolors; i++)
                                interesting_colors.push_back(w.color.mycolors[i]);
                        break;
                }
        }
        if (interesting_colors.size() == 0) {
                // Make dummy interesting color (without this, synchronizer's get_next doesn't proceed) 
                interesting_colors.push_back((ColorID)DUMMY_INTERESTING_COLOR);
        }
}

void HashMap::init_fuzzylog_client(std::vector<std::string>* log_addr, uint8_t txn_version) {
        // XXX This color is not used
        struct colors* c;
        c = (struct colors*)malloc(sizeof(struct colors));
        c->numcolors = 1;
        c->mycolors = (ColorID*)malloc(sizeof(ColorID));
        c->mycolors[0] = (ColorID)DUMMY_INTERESTING_COLOR;
        // Initialize fuzzylog connection
        if (log_addr->size() == 1) {
                const char *server_ip = log_addr->at(0).c_str();
                const char *server_ips[] = { server_ip };
                if (txn_version == txn_protocol::INIT) 
                        m_fuzzylog_client_for_put = new_dag_handle(NULL, 1, server_ips, c);
                else if (txn_version == txn_protocol::SKEENS)
                        m_fuzzylog_client_for_put = new_dag_handle_with_skeens(1, server_ips, c);
        } else {
                if (txn_version == txn_protocol::INIT) {
                        const char *lock_server_ip = log_addr->at(0).c_str();
                        size_t num_chain_servers = log_addr->size() - 1;
                        const char *chain_server_ips[num_chain_servers]; 
                        for (auto i = 0; i < num_chain_servers; i++) {
                                chain_server_ips[i] = log_addr->at(i+1).c_str();
                        }
                        m_fuzzylog_client_for_put = new_dag_handle(lock_server_ip, num_chain_servers, chain_server_ips, c);

                } else if (txn_version == txn_protocol::SKEENS) {
                        size_t num_chain_servers = log_addr->size();
                        const char *chain_server_ips[num_chain_servers]; 
                        for (auto i = 0; i < num_chain_servers; i++) {
                                chain_server_ips[i] = log_addr->at(i).c_str();
                        }
                        m_fuzzylog_client_for_put = new_dag_handle_with_skeens(num_chain_servers, chain_server_ips, c);
                }
        }
}

void HashMap::init_synchronizer(std::vector<std::string>* log_addr, uint8_t txn_version, std::vector<ColorID>& interesting_colors) {
        m_synchronizer = new Synchronizer(log_addr, txn_version, interesting_colors);
        m_synchronizer->run();
}

HashMap::~HashMap() {
        m_synchronizer->join();
        close_dag_handle(m_fuzzylog_client_for_put);
}

uint32_t HashMap::get(uint32_t key) {
        uint32_t val;
        std::condition_variable cv;
        std::atomic_bool cv_spurious_wake_up;
        std::mutex* mtx;

        cv_spurious_wake_up = true;
        mtx = m_synchronizer->get_local_map_lock();
        std::unique_lock<std::mutex> lock(*mtx);
        m_synchronizer->enqueue_get(&cv, &cv_spurious_wake_up);
        cv.wait(lock, [&cv_spurious_wake_up]{ return cv_spurious_wake_up != true; });

        val = m_synchronizer->get(key);
        lock.unlock();

        // Cache value
        m_cache[key] = val;        

        return val;
}

void HashMap::put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        uint64_t data = ((uint64_t)key << 32) | value;
        // For latency measurement 
        auto start_time = std::chrono::system_clock::now();
        append(m_fuzzylog_client_for_put, (char *)&data, sizeof(data), op_color, dep_color);
        auto end_time = std::chrono::system_clock::now();
        auto latency = end_time - start_time; 
        m_latencies.push_back(latency);
}

void HashMap::remove(uint32_t key, struct colors* op_color) {
        // Note: remove is the same as putting a zero value
        uint64_t data = (uint64_t)key << 32;
        append(m_fuzzylog_client_for_put, (char *)&data, sizeof(data), op_color, NULL);

        // Remove cache
        m_cache.erase(key);
}

void HashMap::async_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        // For latency measurement 
        auto start_time = std::chrono::system_clock::now();
        // Async append 
        uint64_t data = ((uint64_t)key << 32) | value;
        write_id wid = async_append(m_fuzzylog_client_for_put, (char *)&data, sizeof(data), op_color, dep_color);
        new_write_id nwid;
        nwid.id = wid;
        // Mark start time
        m_start_time_map[nwid] = start_time;
}

void HashMap::flush_completed_puts() {
        // Flush completed requests best-effort
        flush_completed_appends(m_fuzzylog_client_for_put);
}

new_write_id HashMap::try_wait_for_any_put() {
        // Wait for any append to completed
        write_id wid = try_wait_for_any_append(m_fuzzylog_client_for_put);
        new_write_id nwid;
        nwid.id = wid; 

        if (nwid == NEW_WRITE_ID_NIL) return nwid;      // no more pending appends 

        // Measure latency
        auto searched = m_start_time_map.find(nwid);
        assert(searched != m_start_time_map.end());
        auto end_time = std::chrono::system_clock::now();
        auto latency = end_time - searched->second;
        m_latencies.push_back(latency);
        m_start_time_map.erase(nwid);
        
        return nwid;
}

new_write_id HashMap::wait_for_any_put() {
        // Wait for any append to completed
        write_id wid = wait_for_any_append(m_fuzzylog_client_for_put);
        new_write_id nwid;
        nwid.id = wid; 

        if (nwid == NEW_WRITE_ID_NIL) return nwid;      // no more pending appends 

        // Measure latency
        auto searched = m_start_time_map.find(nwid);
        assert(searched != m_start_time_map.end());
        auto end_time = std::chrono::system_clock::now();
        auto latency = end_time - searched->second;
        m_latencies.push_back(latency);
        m_start_time_map.erase(nwid);
        
        return nwid;
}

void HashMap::wait_for_all_puts() {
        wait_for_all_appends(m_fuzzylog_client_for_put);
}
