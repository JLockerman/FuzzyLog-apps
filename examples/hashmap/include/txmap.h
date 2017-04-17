#pragma once

#include <map.h>
#include <txmap_synchronizer.h>

class TXMap : public BaseMap {
private:
        // Reader thread
        struct colors                                   m_op_color;
        TXMapSynchronizer*                              m_synchronizer; 
        std::atomic<uint64_t>                           m_commit_record_id;

public:
        TXMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, bool replication);
        ~TXMap();

        void get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors);
        void init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication);
        void init_op_color(std::vector<ColorID>& interesting_colors);

        uint64_t get(uint64_t key);

        void serialize_commit_record(txmap_set *rset, txmap_set *wset, char* out, size_t* out_size);
        void execute_move_txn(uint64_t from_key, uint64_t to_key);
};
