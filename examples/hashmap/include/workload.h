#pragma once

#include <hashmap.h>
#include <config.h>

using namespace std;

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

// Transaction class which is aware of HashMap
class Txn {
public:
        typedef enum {
                GET = 0,
                PUT = 1,
        } optype;
public:
        Txn() {}
        virtual ~Txn(){}
        virtual void Run() = 0;
        virtual void AsyncRun() = 0;
        virtual optype op_type() = 0; 
};

class ycsb_insert : public Txn {
private:
        HashMap*                                m_map;
        uint32_t                                m_start;
        uint32_t                                m_end;
        struct colors*                          m_color;
        Context*                                m_context;
        optype                                  m_op_type;
public:
        ycsb_insert(HashMap* map, struct colors* color, uint32_t start, uint32_t end, Context* context, optype op_type) {
                this->m_map = map;
                this->m_color = color;
                this->m_start = start;
                this->m_end = end;
                this->m_context = context;
                this->m_op_type = op_type;
        }
        ~ycsb_insert(){}
        virtual void Run();
        virtual void AsyncRun();
        virtual optype op_type() {
                return m_op_type;
        }
};

class ycsb_read : public Txn {
private:
        HashMap*                                m_map;
        uint32_t                                m_start;
        uint32_t                                m_end;
        struct colors*                          m_color;
        Context*                                m_context;
        optype                                  m_op_type;
public:
        ycsb_read(HashMap* map, struct colors* color, uint32_t start, uint32_t end, Context* context, optype op_type) {
                this->m_map = map;
                this->m_color = color;
                this->m_start = start;
                this->m_end = end;
                this->m_context = context;
                this->m_op_type = op_type;
        }
        ~ycsb_read(){}
        virtual void Run();
        virtual void AsyncRun();
        virtual optype op_type() {
                return m_op_type;
        }
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
