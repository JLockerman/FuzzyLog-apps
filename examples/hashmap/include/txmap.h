#pragma once

#include <map.h>
#include <txmap_synchronizer.h>

typedef struct txmap_record {
        uint64_t key;
        uint64_t value;
        LocationInColor version;  
} txmap_record;

typedef struct txmap_set {
        uint32_t num_entry;
        txmap_record* set;
} txmap_set;

class TXMap : public BaseMap {
private:
        // Reader thread
        TXMapSynchronizer*                          m_synchronizer; 

public:
        TXMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, bool replication);
        ~TXMap();

        void get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors);
        void init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication);

        uint64_t get(uint64_t key);

        void beginTX();
        bool endTX(txmap_set* read_set, txmap_set* write_set);
};
