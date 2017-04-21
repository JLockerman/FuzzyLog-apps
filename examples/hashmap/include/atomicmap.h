#pragma once

#include <map.h>
#include <atomicmap_synchronizer.h>

class AtomicMap : public BaseMap {
private:
        // Reader thread
        AtomicMapSynchronizer*                          m_synchronizer; 

public:
        AtomicMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, bool replication);
        ~AtomicMap();

        bool get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors);
        void init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication);

        void multiput(uint64_t key, uint64_t value, struct colors* op_color, struct colors* dep_color);
        write_id async_multiput(uint64_t key, uint64_t value, struct colors* op_color);
        uint64_t get(uint64_t key);
};
