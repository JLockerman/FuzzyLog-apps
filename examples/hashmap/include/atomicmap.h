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

        uint64_t get(uint64_t key);
};
