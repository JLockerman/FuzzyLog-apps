#include <capmap_tester.h>
#include <cstring>
#include <cassert>
#include <thread>

CAPMapTester::~CAPMapTester() {
        for (size_t i = 0; i < m_num_txns; i++) {
                ycsb_cross_insert* txn = dynamic_cast<ycsb_cross_insert*>(m_txns[i]);
                delete txn;
        }
        delete m_txns;
}

void CAPMapTester::run() {
        int err;
        err = pthread_create(&m_thread, NULL, bootstrap, this);
        assert(err == 0);
}

void CAPMapTester::join() {
        pthread_join(m_thread, NULL);
}

void* CAPMapTester::bootstrap(void *arg) {
        CAPMapTester *worker = static_cast<CAPMapTester*>(arg);
        CAPMap::ProtocolVersion protocol = worker->m_capmap->get_protocol_version();
        if (protocol == CAPMap::ProtocolVersion::VERSION_1) {
                worker->ExecuteProtocol1();

        } else if (protocol == CAPMap::ProtocolVersion::VERSION_2) { 
                std::string role = worker->m_capmap->get_role();
                worker->ExecuteProtocol2(role);

        } else {
                assert(false);
        }
        return NULL;
}

void CAPMapTester::ExecuteProtocol1() {
        uint32_t i;
        uint32_t num_pending = 0; 

        if (m_async) {
                for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                        // try get completed
                        num_pending -= try_get_completed();
        
                        // throttle
                        if (is_throttled()) continue;
                                                
                        // issue
                        if (num_pending == m_window_size) {
                                write_id wid = m_map->wait_for_any_put();
                                if (wid_equality{}(wid, WRITE_ID_NIL)) {
                                        std::cout << "no pending writes. terminate" << std::endl; 
                                        break;
                                }
                                num_pending -= 1;
                                m_context->inc_num_executed();
                        }
                        assert(m_txns[i]->op_type() == Txn::optype::PUT);
                        CAPMap::PartitionStatus partition_status = m_capmap->get_network_partition_status();

                        if (partition_status == CAPMap::PartitionStatus::PARTITIONED) {
                                // Append Weak node
                                m_txns[i]->AsyncWeaklyConsistentRun();
                                num_pending += 1;

                        } else if (partition_status == CAPMap::PartitionStatus::NORMAL) {
                                // Append Strong node
                                m_txns[i]->AsyncStronglyConsistentRun();
                                num_pending += 1;

                        } else if (partition_status == CAPMap::PartitionStatus::HEALING) {
                                // TODO: do nothing. or sleep a bit?
                        }
                }
                m_map->wait_for_all_puts();
                // TODO: add latecy stat for all remaining requests?                
                m_context->set_finished();

                std::cout << "total executed: " << m_context->get_num_executed() << std::endl;

        } else {
                // Repeat request while flag = true
                for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                        m_txns[i]->Run();
                }
                m_context->set_finished();

                std::cout << "total executed: " << m_context->get_num_executed() << std::endl;
        }
}

void CAPMapTester::ExecuteProtocol2(std::string& role) {
        assert(role == "primary" || role == "secondary");

        uint32_t i;
        uint32_t num_pending = 0; 
        bool partitioning_node_appended = false;
        bool healing_node_appended = false;

        reset_throttler();

        if (m_async) {
                for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                        // try get completed
                        num_pending -= try_get_completed();
                                                
                        // issue
                        if (num_pending == m_window_size) {
                                write_id wid = m_map->wait_for_any_put();
                                if (wid_equality{}(wid, WRITE_ID_NIL)) {
                                        std::cout << "no pending writes. terminate" << std::endl; 
                                        break;
                                }
                                num_pending -= 1;
                                m_context->inc_num_executed();
                                // Add latency stat
                                auto start_time = m_context->mark_ended(wid); 
                                add_put_latency(start_time);
                        }

                        if (m_txns[i]->op_type() == Txn::optype::PUT) {
                                CAPMap::PartitionStatus partition_status = m_capmap->get_network_partition_status();
                                if (role == "primary") {
                                        if (partition_status == CAPMap::PartitionStatus::PARTITIONED) {
                                                // Throttle local append
                                                if (is_throttled()) continue;

                                                // Append node
                                                m_txns[i]->AsyncRun();
                                                m_num_issued++;
                                                num_pending += 1;

                                        } else if (partition_status == CAPMap::PartitionStatus::NORMAL) {
                                                // Throttle local append
                                                if (is_throttled()) continue;

                                                // Append node
                                                m_txns[i]->AsyncRun();
                                                m_num_issued++;
                                                num_pending += 1;

                                        } else if (partition_status == CAPMap::PartitionStatus::HEALING) {
                                                // TODO: do nothing. or sleep a bit?
                                        }
                                } else if (role == "secondary") {
                                        if (partition_status == CAPMap::PartitionStatus::PARTITIONED) {
                                                // TODO: the first node after partition should be causally appended after the latest known primary node
                                                if (!partitioning_node_appended) {
                                                        reset_throttler();
                                                        dynamic_cast<ycsb_cross_insert*>(m_txns[i])->AsyncPartitioningAppend();
                                                        partitioning_node_appended = true;
                                                } else {

                                                        // Throttle local append
                                                        if (is_throttled()) continue;

                                                        // Append node
                                                        m_txns[i]->AsyncRun();
                                                }
                                                m_num_issued++;
                                                num_pending += 1;

                                        } else if (partition_status == CAPMap::PartitionStatus::NORMAL) {
                                                healing_node_appended = false;
                                                partitioning_node_appended = false;
                                                // Append node
                                                m_txns[i]->AsyncRemoteRun();
                                                num_pending += 1;

                                        } else if (partition_status == CAPMap::PartitionStatus::HEALING) {
                                                // Append one healing node, then do nothing until the partition status comes back to normal
                                                if (!healing_node_appended) {
                                                        dynamic_cast<ycsb_cross_insert*>(m_txns[i])->AsyncHealingAppend();
                                                        healing_node_appended = true;
                                                } else {
                                                        m_txns[i]->AsyncRemoteRun();
                                                }
                                                num_pending += 1;
                                        }
                                }
                        } else if (m_txns[i]->op_type() == Txn::optype::GET) {
                                auto start_time = std::chrono::system_clock::now();
                                dynamic_cast<ycsb_cross_read*>(m_txns[i])->Run(); 
                                add_get_latency(start_time);
                        }
                }
                m_map->wait_for_all_puts();
                m_context->set_finished();

                std::cout << "total executed: " << m_context->get_num_executed() << std::endl;

        } else {
                // Repeat request while flag = true
                for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                        m_txns[i]->Run();
                }
                m_context->set_finished();

                std::cout << "total executed: " << m_context->get_num_executed() << std::endl;
        }
}
