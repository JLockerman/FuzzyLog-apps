#include <map.h>

#define DUMMY_INTERESTING_COLOR         100000

void BaseMap::init_fuzzylog_client(std::vector<std::string>* log_addr) {
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

void BaseMap::put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        uint64_t data = ((uint64_t)key << 32) | value;
        append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, dep_color);
}

void BaseMap::remove(uint32_t key, struct colors* op_color) {
        // XXX: remove is the same as putting a zero value
        uint64_t data = (uint64_t)key << 32;
        append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);
}

void BaseMap::async_put(uint32_t key, uint32_t value, struct colors* op_color) {
        // Async append 
        uint64_t data = ((uint64_t)key << 32) | value;
        async_append(m_fuzzylog_client, (char *)&data, sizeof(data), op_color, NULL);
}

void BaseMap::flush_completed_puts() {
        // Flush completed requests with best-effort
        flush_completed_appends(m_fuzzylog_client);
}

new_write_id BaseMap::try_wait_for_any_put() {
        // Wait for any append to be completed (non-blocking)
        write_id wid = try_wait_for_any_append(m_fuzzylog_client);
        new_write_id nwid;
        nwid.id = wid; 
        return nwid;
}

new_write_id BaseMap::wait_for_any_put() {
        // Wait for any append to be completed (blocking)
        write_id wid = wait_for_any_append(m_fuzzylog_client);
        new_write_id nwid;
        nwid.id = wid; 
        return nwid;
}

void BaseMap::wait_for_all_puts() {
        wait_for_all_appends(m_fuzzylog_client);
}
