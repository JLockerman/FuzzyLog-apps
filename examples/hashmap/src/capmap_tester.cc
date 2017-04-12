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
        CAPMap::ProtocolVersion protocol = worker->m_map->get_protocol_version();
        if (protocol == CAPMap::ProtocolVersion::VERSION_1) {
                worker->ExecuteProtocol1();

        } else if (protocol == CAPMap::ProtocolVersion::VERSION_2) { 
                std::string role = worker->m_map->get_role();
                worker->ExecuteProtocol2(role);

        } else {
                assert(false);
        }
        return NULL;
}

pthread_t* CAPMapTester::get_pthread_id() {
        return &m_thread;
}

uint32_t CAPMapTester::try_get_completed() {
        uint32_t num_completed = 0;
        while (true) {
                write_id wid = m_map->try_wait_for_any_put();
                if (wid_equality{}(wid, WRITE_ID_NIL)) 
                        break; 
                m_context->inc_num_executed();
                num_completed += 1;
                // Add latency stat
                auto start_time = m_context->mark_ended(wid); 
                add_put_latency(start_time);
        }
        return num_completed;
}

void CAPMapTester::ExecuteProtocol1() {
        uint32_t i;
        uint32_t num_pending = 0; 

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
                        }
                        assert(m_txns[i]->op_type() == Txn::optype::PUT);
                        CAPMap::PartitionStatus partition_status = m_map->get_network_partition_status();

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
        uint32_t sleep_between_txn = 0;         // in nanocsecond
        // Throttle txn issuing rate if specified 
        if (m_txn_rate > 0) {
                sleep_between_txn = 1000000000 / m_txn_rate; 
        }

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
                                CAPMap::PartitionStatus partition_status = m_map->get_network_partition_status();
                                if (role == "primary") {
                                        if (partition_status == CAPMap::PartitionStatus::PARTITIONED) {
                                                // Only throttle while local appending in secondary
                                                if (sleep_between_txn > 0)
                                                        std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_between_txn));

                                                // Append node
                                                m_txns[i]->AsyncRun();
                                                num_pending += 1;

                                        } else if (partition_status == CAPMap::PartitionStatus::NORMAL) {
                                                // Only throttle while local appending in secondary
                                                if (sleep_between_txn > 0)
                                                        std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_between_txn));

                                                // Append node
                                                m_txns[i]->AsyncRun();
                                                num_pending += 1;

                                        } else if (partition_status == CAPMap::PartitionStatus::HEALING) {
                                                // TODO: do nothing. or sleep a bit?
                                        }
                                } else if (role == "secondary") {
                                        if (partition_status == CAPMap::PartitionStatus::PARTITIONED) {
                                                // Only throttle while local appending in secondary
                                                if (sleep_between_txn > 0)
                                                        std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_between_txn));

                                                // TODO: the first node after partition should be causally appended after the latest known primary node
                                                if (!partitioning_node_appended) {
                                                        dynamic_cast<ycsb_cross_insert*>(m_txns[i])->AsyncPartitioningAppend();
                                                        partitioning_node_appended = true;
                                                } else {
                                                        // Append node
                                                        m_txns[i]->AsyncRun();
                                                }
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
uint64_t CAPMapTester::get_num_executed() {
        return m_context->get_num_executed();
}

void CAPMapTester::add_put_latency(std::chrono::system_clock::time_point issue_time) {
        latency_footprint lf;
        // issue time
        lf.m_issue_time = issue_time;
        // latency
        auto end_time = std::chrono::system_clock::now();
        std::chrono::duration<uint64_t, std::nano> elapsed = end_time - issue_time;
        lf.m_latency = elapsed.count();
        // add
        m_put_latencies.push_back(lf);
}

void CAPMapTester::add_get_latency(std::chrono::system_clock::time_point issue_time) {
        latency_footprint lf;
        // issue time
        lf.m_issue_time = issue_time;
        // latency
        auto end_time = std::chrono::system_clock::now();
        std::chrono::duration<uint64_t, std::nano> elapsed = end_time - issue_time;
        lf.m_latency = elapsed.count();
        // add
        m_get_latencies.push_back(lf);
}

void CAPMapTester::get_put_latencies(std::vector<latency_footprint> &latencies) {
        for (auto l : m_put_latencies) {
                latencies.push_back(l);
        }
}

void CAPMapTester::get_get_latencies(std::vector<latency_footprint> &latencies) {
        for (auto l : m_get_latencies) {
                latencies.push_back(l);
        }
}
