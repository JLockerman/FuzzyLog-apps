#include <map.h>
#include <thread>
#include <cassert>

#define DUMMY_INTERESTING_COLOR         100000

void BaseMap::init_fuzzylog_client(std::vector<std::string>* log_addr) {
        // XXX This color is not used
        struct colors* c;
        c = static_cast<struct colors*>(malloc(sizeof(struct colors)));
        c->numcolors = 1;
        c->mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID)));
        c->mycolors[0] = static_cast<ColorID>(DUMMY_INTERESTING_COLOR);
        // Initialize fuzzylog connection
        if (m_replication) {
                assert (log_addr->size() > 0 && log_addr->size() % 2 == 0);
                size_t num_chain_servers = log_addr->size() / 2;
                const char *chain_server_head_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_head_ips[i] = log_addr->at(i).c_str();
                }
                const char *chain_server_tail_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_tail_ips[i] = log_addr->at(num_chain_servers+i).c_str();
                }
                m_fuzzylog_client = new_dag_handle_with_replication(num_chain_servers, chain_server_head_ips, chain_server_tail_ips, c);

        } else {
                size_t num_chain_servers = log_addr->size();
                const char *chain_server_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_ips[i] = log_addr->at(i).c_str();
                }
                m_fuzzylog_client = new_dag_handle_with_skeens(num_chain_servers, chain_server_ips, c);
        }
        std::this_thread::sleep_for(std::chrono::seconds(3));
}

void BaseMap::put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        uint64_t data = ((uint64_t)key << 32) | value;
        append(m_fuzzylog_client, reinterpret_cast<char*>(&data), sizeof(data), op_color, dep_color);
}

void BaseMap::remove(uint32_t key, struct colors* op_color) {
        // XXX: remove is the same as putting a zero value
        uint64_t data = (uint64_t)key << 32;
        append(m_fuzzylog_client, reinterpret_cast<char*>(&data), sizeof(data), op_color, NULL);
}

write_id BaseMap::async_put(uint32_t key, uint32_t value, struct colors* op_color) {
        // Async append 
        uint64_t data = ((uint64_t)key << 32) | value;
        return async_append(m_fuzzylog_client, reinterpret_cast<char*>(&data), sizeof(data), op_color, NULL);
}

void BaseMap::flush_completed_puts() {
        // Flush completed requests with best-effort
        flush_completed_appends(m_fuzzylog_client);
}

write_id BaseMap::try_wait_for_any_put() {
        // Wait for any append to be completed (non-blocking)
        return try_wait_for_any_append(m_fuzzylog_client);
}

write_id BaseMap::wait_for_any_put() {
        // Wait for any append to be completed (blocking)
        return wait_for_any_append(m_fuzzylog_client);
}

void BaseMap::wait_for_all_puts() {
        wait_for_all_appends(m_fuzzylog_client);
}
