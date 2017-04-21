#include <map.h>
#include <thread>
#include <cassert>

#define DUMMY_INTERESTING_COLOR         100000

BaseMap::BaseMap(std::vector<std::string>* log_addr, bool replication): m_replication(replication) {
        init_fuzzylog_client(log_addr);
}

BaseMap::~BaseMap() {
        close_dag_handle(m_fuzzylog_client);
}

void BaseMap::init_fuzzylog_client(std::vector<std::string>* log_addr) {
        // XXX This color is not used
        struct colors* c;
        c = static_cast<struct colors*>(malloc(sizeof(struct colors)));
        c->numcolors = 1;
        c->mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID)));
        c->mycolors[0] = static_cast<ColorID>(DUMMY_INTERESTING_COLOR);
        // Initialize fuzzylog connection
        m_fuzzylog_client = BaseMap::get_connection(log_addr, c, m_replication);
        std::this_thread::sleep_for(std::chrono::seconds(3));
}

DAGHandle* BaseMap::get_connection(std::vector<std::string>* log_addr, struct colors* c, bool replication) {
        if (replication) {
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
                return new_dag_handle_with_replication(num_chain_servers, chain_server_head_ips, chain_server_tail_ips, c);

        } else {
                size_t num_chain_servers = log_addr->size();
                const char *chain_server_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_ips[i] = log_addr->at(i).c_str();
                }
                return new_dag_handle_with_skeens(num_chain_servers, chain_server_ips, c);
        }
} 

void BaseMap::put(uint64_t key, uint64_t value, struct colors* op_color, struct colors* dep_color) {
        uint64_t *data = reinterpret_cast<uint64_t*>(m_buf);
        data[0] = key;
        data[1] = value;
        size_t data_size = sizeof(uint64_t) * 2;
        append(m_fuzzylog_client, m_buf, data_size, op_color, dep_color);
}

void BaseMap::remove(uint64_t key, struct colors* op_color) {
        put(key, 0, op_color, NULL);
}

write_id BaseMap::async_put(uint64_t key, uint64_t value, struct colors* op_color) {
        // Async append 
        uint64_t *data = reinterpret_cast<uint64_t*>(m_buf);
        data[0] = key;
        data[1] = value;
        size_t data_size = sizeof(uint64_t) * 2;
        return async_append(m_fuzzylog_client, m_buf, data_size, op_color, NULL);
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
