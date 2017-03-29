#include <hashmap.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unistd.h>

#define DUMMY_INTERESTING_COLOR         100000

HashMap::HashMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload) {
        // find interesting colors from get workload
        m_synchronizer = NULL;
        init_fuzzylog_client(log_addr); 

        std::vector<ColorID> interesting_colors;
        if (get_interesting_colors(workload, interesting_colors))
                init_synchronizer(log_addr, interesting_colors); 
}

bool HashMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors) {
        bool get_workload_found = false;
        uint32_t i;
        for (auto w : *workload) {
                if (w.op_type == "get") {
                        for (i = 0; i < w.color.numcolors; i++) {
                                interesting_colors.push_back(w.color.mycolors[i]);
                        }
                        get_workload_found = true;
                        break;
                }
        }
        return get_workload_found;
}

void HashMap::init_fuzzylog_client(std::vector<std::string>* log_addr) {
        // XXX This color is not used
        struct colors* c;
        c = (struct colors*)malloc(sizeof(struct colors));
        c->numcolors = 1;
        c->mycolors = (ColorID*)malloc(sizeof(ColorID));
        c->mycolors[0] = (ColorID)DUMMY_INTERESTING_COLOR;
        // Initialize fuzzylog connection
        size_t num_chain_servers = log_addr->size();
        const char *chain_server_ips[num_chain_servers]; 
        for (auto i = 0; i < num_chain_servers; i++) {
                chain_server_ips[i] = log_addr->at(i).c_str();
        }
        m_fuzzylog_client = new_dag_handle_with_skeens(num_chain_servers, chain_server_ips, c);
}

void HashMap::init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors) {
        m_synchronizer = new Synchronizer(log_addr, interesting_colors);
        m_synchronizer->run();
}

HashMap::~HashMap() {
        if (m_synchronizer != NULL)
                m_synchronizer->join();
        close_dag_handle(m_fuzzylog_client);
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

        return val;
}

void HashMap::put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        uint64_t data = ((uint64_t)key << 32) | value;
        append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, dep_color);
}

void HashMap::remove(uint32_t key, struct colors* op_color) {
        // XXX: remove is the same as putting a zero value
        uint64_t data = (uint64_t)key << 32;
        append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);
}

void HashMap::async_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        // Async append 
        uint64_t data = ((uint64_t)key << 32) | value;
        async_append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, dep_color);
}

void HashMap::flush_completed_puts() {
        // Flush completed requests with best-effort
        flush_completed_appends(m_fuzzylog_client);
}

new_write_id HashMap::try_wait_for_any_put() {
        // Wait for any append to be completed (non-blocking)
        write_id wid = try_wait_for_any_append(m_fuzzylog_client);
        new_write_id nwid;
        nwid.id = wid; 
        return nwid;
}

new_write_id HashMap::wait_for_any_put() {
        // Wait for any append to be completed (blocking)
        write_id wid = wait_for_any_append(m_fuzzylog_client);
        new_write_id nwid;
        nwid.id = wid; 
        return nwid;
}

void HashMap::wait_for_all_puts() {
        wait_for_all_appends(m_fuzzylog_client);
}
