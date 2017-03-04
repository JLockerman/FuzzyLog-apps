#pragma once

#include <hashmap.h>
#include <config.h>

using namespace std;

// Transaction class which is aware of HashMap
class Txn {
public:
        Txn() {}
        virtual ~Txn(){}
        virtual void Run() = 0;
        virtual void AsyncRun() = 0;
        virtual new_write_id wait_for_any_op() = 0;
        virtual void wait_for_all_ops() = 0;
        virtual void flush_completed_ops() = 0;
};

class Context {
private:
        std::atomic<uint64_t>                   m_num_executed;
        std::atomic<bool>                       m_finished;
public:
        Context() {
                this->m_num_executed = 0;
                this->m_finished = false;
        }
        bool is_finished() {
                return m_finished;
        }
        void set_finished() {
                m_finished = true;
        }
        uint64_t get_num_executed() {
                return m_num_executed;
        }
        void inc_num_executed() {
                m_num_executed++; 
        }
};

class ycsb_insert : public Txn {
private:
        HashMap*                                m_map;
        uint32_t                                m_start;
        uint32_t                                m_end;
        struct colors*                          m_color;
        Context*                                m_context;
public:
        ycsb_insert(HashMap* map, struct colors* color, uint32_t start, uint32_t end, Context* context) {
                this->m_map = map;
                this->m_color = color;
                this->m_start = start;
                this->m_end = end;
                this->m_context = context;
        }
        ~ycsb_insert(){}
        virtual void Run();
        virtual void AsyncRun();
        virtual new_write_id wait_for_any_op();
        virtual void wait_for_all_ops();
        virtual void flush_completed_ops();
};

class workload_generator {
private:
        Context*                                m_context;
        HashMap*                                m_map;
        uint32_t                                m_range;
        vector<workload_config>*                m_workload;
public:
        workload_generator(Context* context, HashMap* map, uint32_t range, vector<workload_config>* workload) {
                this->m_context = context;
                this->m_map = map;
                this->m_range = range;
                this->m_workload = workload;
        }
        Txn** Gen();
};
