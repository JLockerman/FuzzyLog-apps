#include <capmap.h>
#include <cassert>

CAPMap::CAPMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, ProtocolVersion protocol, std::string& role, bool replication): BaseMap(log_addr, replication), m_protocol(protocol), m_role(role), m_network_partition_status(NORMAL), m_synchronizer(NULL) {
        // FIXME: m_map_type
        std::vector<uint64_t> interesting_colors;
        if (get_interesting_colors(workload, interesting_colors))
                init_synchronizer(log_addr, interesting_colors, replication);
}

CAPMap::~CAPMap() {
        if (m_synchronizer != NULL)
                m_synchronizer->join();
        fuzzylog_close(m_fuzzylog_client);
}

bool CAPMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<uint64_t>& interesting_colors) {
        bool get_workload_found = false;
        for (auto w : *workload) {
                if (w.op_type == "get") {
                        assert(w.colors.num_remote_chains == 2);
                        interesting_colors.push_back(w.colors.remote_chains[0]);
                        interesting_colors.push_back(w.colors.remote_chains[1]);
                        get_workload_found = true;
                        break;
                }
        }
        return get_workload_found;

}

void CAPMap::init_synchronizer(std::vector<std::string>* log_addr, std::vector<uint64_t>& interesting_colors, bool replication) {
        m_synchronizer = new CAPMapSynchronizer(this, log_addr, interesting_colors, replication);
        m_synchronizer->run();
}

uint64_t CAPMap::get(uint64_t key) {
        uint64_t val;
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

void CAPMap::get_payload(uint64_t key, uint64_t value, uint8_t flag, char* out, size_t* out_size) {
        struct Node* node = (struct Node*)out;

        node->key = key;
        node->value = value;
        node->flag = flag;
        // timestamp
        using namespace std::chrono;
        auto now = system_clock::now();
        auto now_ms = time_point_cast<milliseconds>(now);
        auto now_ms_since_epoch = now_ms.time_since_epoch();
        long duration = now_ms_since_epoch.count();
        node->ts = duration;
        *out_size = sizeof(struct Node);
}

void CAPMap::get_payload_for_normal_node(uint64_t key, uint64_t value, char* out, size_t* out_size) {
        get_payload(key, value, NormalNode, out, out_size);
}

void CAPMap::get_payload_for_healing_node(uint64_t key, uint64_t value, char* out, size_t* out_size) {
        get_payload(key, value, HealingNode, out, out_size);
}

void CAPMap::get_payload_for_partitioning_node(uint64_t key, uint64_t value, char* out, size_t* out_size) {
        get_payload(key, value, PartitioningNode, out, out_size);
}

WriteId CAPMap::async_normal_put(uint64_t key, uint64_t value, ColorSpec op_color) {
        size_t size;
        char buf[sizeof(struct Node)];
        get_payload_for_normal_node(key, value, &buf[0], &size);
        return fuzzylog_async_append(m_fuzzylog_client, &buf[0], size, &op_color, 1);
}

WriteId CAPMap::async_partitioning_put(uint64_t key, uint64_t value, ColorSpec op_color) {
        size_t size;
        char buf[sizeof(struct Node)];
        get_payload_for_partitioning_node(key, value, &buf[0], &size);
        return fuzzylog_async_append(m_fuzzylog_client, &buf[0], size, &op_color, 1);
}

WriteId CAPMap::async_healing_put(uint64_t key, uint64_t value, ColorSpec rev_colors) {
        size_t size;
        char buf[sizeof(struct Node)];
        ColorSpec color = {.local_chain = rev_colors.remote_chains[0]};
        get_payload_for_healing_node(key, value, &buf[0], &size);
        return fuzzylog_async_append(m_fuzzylog_client, &buf[0], size, &color, 1);  // TODO: check if dep_color would depend on the latest node appended by the same machine
}
