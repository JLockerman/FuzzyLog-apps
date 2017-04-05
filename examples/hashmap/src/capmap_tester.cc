#include <capmap_tester.h>
#include <cstring>
#include <cassert>

CAPMapTester::~CAPMapTester() {
        for (size_t i = 0; i < m_num_txns; i++) {
                ycsb_cross_insert* txn = (ycsb_cross_insert*)m_txns[i];
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
        CAPMapTester *worker = (CAPMapTester*)arg;
        worker->Execute();
        return NULL;
}

pthread_t* CAPMapTester::get_pthread_id() {
        return &m_thread;
}

uint32_t CAPMapTester::try_get_completed() {
        uint32_t num_completed = 0;
        while (true) {
                new_write_id nwid = m_map->try_wait_for_any_put();
                //std::cout << "try_wait:" << nwid.id.p1 << std::endl; 
                if (nwid == NEW_WRITE_ID_NIL)
                        break; 
                m_context->inc_num_executed();
                num_completed += 1;
                // TODO: manage map for issued request                
        }
        return num_completed;
}

void CAPMapTester::Execute() {
        uint32_t i;
        uint32_t num_pending = 0; 

        if (m_async) {
                for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                        // try get completed
                        num_pending -= try_get_completed();
                                                
                        // issue
                        if (num_pending == m_window_size) {
                                new_write_id nwid = m_map->wait_for_any_put();
                                if (nwid == NEW_WRITE_ID_NIL) {
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
                                m_txns[i]->TryAsyncStronglyConsistentRun();
                                num_pending += 1;

                        } else if (partition_status == CAPMap::PartitionStatus::REORDER_WEAK_NODES) {
                                // TODO: do nothing. or sleep a bit?
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
