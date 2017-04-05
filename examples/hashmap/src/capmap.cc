#include <capmap.h>

char buf[DELOS_MAX_DATA_SIZE];

CAPMap::CAPMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload): BaseMap(log_addr), m_network_partition_status(NORMAL), m_synchronizer(NULL) {
        // FIXME: m_map_type
        std::vector<ColorID> interesting_colors;
        get_interesting_colors(workload, interesting_colors);
        init_synchronizer(log_addr, interesting_colors); 
}

CAPMap::~CAPMap() {
        if (m_synchronizer != NULL)
                m_synchronizer->join();
        close_dag_handle(m_fuzzylog_client);
}

void CAPMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors) {
        assert(workload->size() == 1);
        workload_config w = workload->at(0); 
        assert(w.op_type == "put");
        assert(w.color.numcolors == 1);
        assert(w.dep_color.numcolors == 1);
        interesting_colors.push_back(w.color.mycolors[0]);
        interesting_colors.push_back(w.dep_color.mycolors[0]);
}

void CAPMap::init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors) {
        m_synchronizer = new CAPMapSynchronizer(this, log_addr, interesting_colors);
        m_synchronizer->run();
}

void CAPMap::get_payload(uint32_t key, uint32_t value, bool strong, char* out, size_t* out_size) {
        struct Node* node = (struct Node*)out;

        node->key = key;
        node->value = value;
        node->flag = strong ? STRONG_FLAG : WEAK_FLAG;
        // timestamp
        using namespace std::chrono;
        auto now = system_clock::now();
        auto now_ms = time_point_cast<milliseconds>(now);
        auto now_ms_since_epoch = now_ms.time_since_epoch();
        long duration = now_ms_since_epoch.count();
        node->ts = duration;
        *out_size = sizeof(struct Node);
}

bool CAPMap::async_strong_depend_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color, bool force_fail) {
        if (force_fail)
                return false;

        assert(dep_color != NULL);
        size_t size;
        get_payload(key, value, true, buf, &size);
        async_append(m_fuzzylog_client, buf, size, op_color, NULL);     // FIXME: dep_color should be set. not working for now
        return true;
}

void CAPMap::async_weak_depend_put(uint32_t key, uint32_t value, struct colors* op_color) {
        size_t size;
        get_payload(key, value, false, buf, &size);
        async_append(m_fuzzylog_client, buf, size, op_color, NULL);
}
