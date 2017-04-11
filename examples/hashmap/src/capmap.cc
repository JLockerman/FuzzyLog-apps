#include <capmap.h>
#include <cassert>

char buf[DELOS_MAX_DATA_SIZE];

CAPMap::CAPMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, ProtocolVersion protocol, std::string& role, bool replication): BaseMap(log_addr, replication), m_protocol(protocol), m_role(role), m_network_partition_status(NORMAL), m_synchronizer(NULL) {
        // FIXME: m_map_type
        std::vector<ColorID> interesting_colors;
        if (get_interesting_colors(workload, interesting_colors))
                init_synchronizer(log_addr, interesting_colors, replication);
}

CAPMap::~CAPMap() {
        if (m_synchronizer != NULL)
                m_synchronizer->join();
        close_dag_handle(m_fuzzylog_client);
}

bool CAPMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors) {
        bool get_workload_found = false;
        uint32_t i;
        for (auto w : *workload) {
                if (w.op_type == "get") {
                        assert(w.first_color.numcolors == 1);
                        assert(w.second_color.numcolors == 1);
                        interesting_colors.push_back(w.first_color.mycolors[0]);
                        interesting_colors.push_back(w.second_color.mycolors[0]);
                        get_workload_found = true;
                        break;
                }
        }
        return get_workload_found;

}

void CAPMap::init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication) {
        m_synchronizer = new CAPMapSynchronizer(this, log_addr, interesting_colors, replication);
        m_synchronizer->run();
}

uint32_t CAPMap::get(uint32_t key) {
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

void CAPMap::get_payload(uint32_t key, uint32_t value, uint32_t flag, char* out, size_t* out_size) {
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

void CAPMap::get_payload_for_strong_node(uint32_t key, uint32_t value, char* out, size_t* out_size) {
        get_payload(key, value, StrongNode, out, out_size);
}

void CAPMap::get_payload_for_weak_node(uint32_t key, uint32_t value, char* out, size_t* out_size) {
        get_payload(key, value, WeakNode, out, out_size);
}

void CAPMap::get_payload_for_normal_node(uint32_t key, uint32_t value, char* out, size_t* out_size) {
        get_payload(key, value, NormalNode, out, out_size);
}

void CAPMap::get_payload_for_healing_node(uint32_t key, uint32_t value, char* out, size_t* out_size) {
        get_payload(key, value, HealingNode, out, out_size);
}

void CAPMap::get_payload_for_partitioning_node(uint32_t key, uint32_t value, char* out, size_t* out_size) {
        get_payload(key, value, PartitioningNode, out, out_size);
}

write_id CAPMap::async_strong_depend_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        size_t size;
        get_payload_for_strong_node(key, value, buf, &size);
        return async_append(m_fuzzylog_client, buf, size, op_color, NULL);     // FIXME: dep_color should be set. not working for now
}

write_id CAPMap::async_weak_put(uint32_t key, uint32_t value, struct colors* op_color) {
        size_t size;
        get_payload_for_weak_node(key, value, buf, &size);
        return async_append(m_fuzzylog_client, buf, size, op_color, NULL);
}

write_id CAPMap::async_normal_put(uint32_t key, uint32_t value, struct colors* op_color) {
        size_t size;
        get_payload_for_normal_node(key, value, buf, &size);
        return async_append(m_fuzzylog_client, buf, size, op_color, NULL); 
}

write_id CAPMap::async_partitioning_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        size_t size;
        get_payload_for_partitioning_node(key, value, buf, &size);
        return async_simple_causal_append(m_fuzzylog_client, buf, size, op_color, dep_color);
}

write_id CAPMap::async_healing_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color) {
        size_t size;
        get_payload_for_healing_node(key, value, buf, &size);
        return async_simple_causal_append(m_fuzzylog_client, buf, size, op_color, dep_color);  // TODO: check if dep_color would depend on the latest node appended by the same machine
}
