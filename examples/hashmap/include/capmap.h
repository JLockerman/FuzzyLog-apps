#pragma once

#include <map.h>
#include <capmap_synchronizer.h>

struct Node {
        uint64_t        key;
        uint64_t        value;
        long            ts; 
        uint8_t        flag;
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
                UNINITIALIZED           = 0,
                NORMAL                  = 1,
                PARTITIONED             = 2,
                HEALING                 = 3,
        };
        enum ProtocolVersion {
                VERSION_1               = 1,
                VERSION_2               = 2,
        };

        static const uint8_t WeakNode         = 11;
        static const uint8_t StrongNode       = 22;
        static const uint8_t HealingNode      = 33;
        static const uint8_t PartitioningNode = 44;
        static const uint8_t NormalNode       = 55;

private:
        ProtocolVersion                         m_protocol;
        std::string                             m_role;
        std::atomic<PartitionStatus>            m_network_partition_status;
        CAPMapSynchronizer*                     m_synchronizer; 

public:
        CAPMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, ProtocolVersion protocol, std::string& role, bool replication);
        ~CAPMap();

        ProtocolVersion get_protocol_version() {
                return m_protocol;
        }
        std::string get_role() {
                return m_role;
        }

        bool get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors);
        void init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, bool replication);

        void set_network_partition_status(PartitionStatus status) {
                switch (status) {
                case UNINITIALIZED:     std::cout << "Switch to UNINITIALIZED\n";       break;
                case NORMAL:            std::cout << "Switch to NORMAL\n";              break;
                case PARTITIONED:       std::cout << "Switch to PARTITIONED\n";         break;
                case HEALING:           std::cout << "Switch to HEALING\n";             break;
                }
                this->m_network_partition_status = status;
        }
        PartitionStatus get_network_partition_status() {
                return m_network_partition_status;
        }
        // get operation
        uint64_t get(uint64_t key);

        // create payload
        void get_payload(uint64_t key, uint64_t value, uint8_t flag, char* out, size_t* out_size);
        // For protocol 1
        void get_payload_for_strong_node(uint64_t key, uint64_t value, char* out, size_t* out_size);
        void get_payload_for_weak_node(uint64_t key, uint64_t value, char* out, size_t* out_size);
        // For protocol 2
        void get_payload_for_normal_node(uint64_t key, uint64_t value, char* out, size_t* out_size);
        void get_payload_for_healing_node(uint64_t key, uint64_t value, char* out, size_t* out_size);
        void get_payload_for_partitioning_node(uint64_t key, uint64_t value, char* out, size_t* out_size);
        
        // Operations for protocol 1
        write_id async_strong_depend_put(uint64_t key, uint64_t value, struct colors* op_color, struct colors* dep_color);
        write_id async_weak_put(uint64_t key, uint64_t value, struct colors* op_color);
        // Operations for protocol 2
        write_id async_normal_put(uint64_t key, uint64_t value, struct colors* op_color);
        write_id async_partitioning_put(uint64_t key, uint64_t value, struct colors* op_color, struct colors* dep_color);        // XXX: should be called only from secondary machine
        write_id async_healing_put(uint64_t key, uint64_t value, struct colors* op_color, struct colors* dep_color);        // XXX: should be called only from secondary machine
};
