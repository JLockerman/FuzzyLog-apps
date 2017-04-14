#pragma once

#include <atomicmap.h>
#include <request.h>
#include <atomicmap_config.h>

class ycsb_insert : public Txn {
private:
        AtomicMap*                              m_map;
        uint64_t                                m_start;
        uint64_t                                m_end;
        struct colors*                          m_color;
        struct colors*                          m_dep_color;
        Context*                                m_context;
        optype                                  m_op_type;
        bool                                    m_is_strong;
public:
        ycsb_insert(AtomicMap* map, struct colors* color, struct colors* dep_color, uint64_t start, uint64_t end, Context* context, optype op_type, bool is_strong) {
                this->m_map = map;
                this->m_color = color;
                this->m_dep_color = dep_color;
                this->m_start = start;
                this->m_end = end;
                this->m_context = context;
                this->m_op_type = op_type;
                this->m_is_strong = is_strong;
        }
        ~ycsb_insert(){}
        virtual void Run();
        virtual write_id AsyncRun();
        virtual write_id AsyncRemoteRun();
        virtual write_id AsyncStronglyConsistentRun();
        virtual write_id AsyncWeaklyConsistentRun();
        virtual optype op_type() {
                return m_op_type;
        }
};

class ycsb_read : public Txn {
private:
        AtomicMap*                              m_map;
        uint64_t                                m_start;
        uint64_t                                m_end;
        struct colors*                          m_color;
        Context*                                m_context;
        optype                                  m_op_type;
public:
        ycsb_read(AtomicMap* map, struct colors* color, uint64_t start, uint64_t end, Context* context, optype op_type) {
                this->m_map = map;
                this->m_color = color;
                this->m_start = start;
                this->m_end = end;
                this->m_context = context;
                this->m_op_type = op_type;
        }
        ~ycsb_read(){}
        virtual void Run();
        virtual write_id AsyncRun();
        virtual write_id AsyncRemoteRun();
        virtual write_id AsyncStronglyConsistentRun();
        virtual write_id AsyncWeaklyConsistentRun();
        virtual optype op_type() {
                return m_op_type;
        }
};

class atomicmap_workload_generator {
private:
        Context*                                m_context;
        AtomicMap*                              m_map;
        uint64_t                                m_range;
        vector<workload_config>*                m_workload;
public:
        atomicmap_workload_generator(Context* context, AtomicMap* map, uint64_t range, vector<workload_config>* workload) {
                this->m_context = context;
                this->m_map = map;
                this->m_range = range;
                this->m_workload = workload;
        }
        Txn** Gen();
};
