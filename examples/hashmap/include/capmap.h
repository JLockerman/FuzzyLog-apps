#pragma once

#include <map.h>
#include <capmap_synchronizer.h>

#define WEAK_FLAG               11
#define STRONG_FLAG             22

struct Node {
        uint32_t        key;
        uint32_t        value;
        uint32_t        flag;
        long            ts; 
        struct Node* clone() {
                struct Node* new_node = (struct Node*)malloc(sizeof(struct Node));
                new_node->key   = this->key;
                new_node->value = this->value;
                new_node->flag  = this->flag;
                new_node->ts    = this->ts;
                return new_node;
        }
};

class CAPMap : public BaseMap {
public:
        enum PartitionStatus {
                NORMAL                  = 1,
                PARTITIONED             = 2,
                REORDER_WEAK_NODES      = 3,
        };
private:
        std::atomic<PartitionStatus>            m_network_partition_status;
        CAPMapSynchronizer*                     m_synchronizer; 

public:
        CAPMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload);
        ~CAPMap();

        void get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors);
        void init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors);

        void set_network_partition_status(PartitionStatus status) {
                this->m_network_partition_status = status;
        }
        PartitionStatus get_network_partition_status() {
                return m_network_partition_status;
        }
        void get_payload(uint32_t key, uint32_t value, bool strong, char* out, size_t* out_size);
        
        // Operations
        bool async_strong_depend_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color, bool force_fail = false);
        void async_weak_depend_put(uint32_t key, uint32_t value, struct colors* op_color);
};
