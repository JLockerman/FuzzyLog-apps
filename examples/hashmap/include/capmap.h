#pragma once

#include <map.h>
#include <capmap_synchronizer.h>

#define WEAK_NODE               11
#define STRONG_NODE             22
#define HEALING_NODE            33
#define PARTITIONING_NODE       44
#define NORMAL_NODE             55

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
                HEALING                 = 3,
        };
        enum ProtocolVersion {
                VERSION_1               = 1,
                VERSION_2               = 2,
        };
private:
        ProtocolVersion                         m_protocol;
        std::string                             m_role;
        std::atomic<PartitionStatus>            m_network_partition_status;
        CAPMapSynchronizer*                     m_synchronizer; 

public:
        CAPMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, ProtocolVersion protocol, std::string& role);
        ~CAPMap();

        ProtocolVersion get_protocol_version() {
                return m_protocol;
        }
        std::string get_role() {
                return m_role;
        }

        void get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors);
        void init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors);

        void set_network_partition_status(PartitionStatus status) {
                switch (status) {
                case NORMAL:            std::cout << "Switch to NORMAL\n";      break;
                case PARTITIONED:       std::cout << "Switch to PARTITIONED\n"; break;
                case HEALING:           std::cout << "Switch to HEALING\n";     break;
                }
                this->m_network_partition_status = status;
        }
        PartitionStatus get_network_partition_status() {
                return m_network_partition_status;
        }
        void get_payload(uint32_t key, uint32_t value, uint32_t flag, char* out, size_t* out_size);
        // For protocol 1
        void get_payload_for_strong_node(uint32_t key, uint32_t value, char* out, size_t* out_size);
        void get_payload_for_weak_node(uint32_t key, uint32_t value, char* out, size_t* out_size);
        // For protocol 2
        void get_payload_for_normal_node(uint32_t key, uint32_t value, char* out, size_t* out_size);
        void get_payload_for_healing_node(uint32_t key, uint32_t value, char* out, size_t* out_size);
        void get_payload_for_partitioning_node(uint32_t key, uint32_t value, char* out, size_t* out_size);
        
        // Operations for protocol 1
        bool async_strong_depend_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color, bool force_fail = false);
        void async_weak_put(uint32_t key, uint32_t value, struct colors* op_color);
        // Operations for protocol 2
        void async_normal_put(uint32_t key, uint32_t value, struct colors* op_color);
        void async_partitioning_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color);        // XXX: should be called only from secondary machine
        void async_healing_put(uint32_t key, uint32_t value, struct colors* op_color, struct colors* dep_color);        // XXX: should be called only from secondary machine
};
