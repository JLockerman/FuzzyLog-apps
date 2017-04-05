#pragma once

#include <capmap.h>
#include <request.h>
#include <config.h>

class ycsb_cross_insert : public Txn {
private:
        CAPMap*                                 m_map;
        uint32_t                                m_start;
        uint32_t                                m_end;
        struct colors*                          m_color;
        struct colors*                          m_dep_color;
        Context*                                m_context;
        optype                                  m_op_type;
public:
        ycsb_cross_insert(CAPMap* map, struct colors* color, struct colors* dep_color, uint32_t start, uint32_t end, Context* context, optype op_type) {
                this->m_map = map;
                this->m_color = color;
                this->m_dep_color = dep_color;
                this->m_start = start;
                this->m_end = end;
                this->m_context = context;
                this->m_op_type = op_type;
        }
        ~ycsb_cross_insert(){}
        virtual void Run();
        virtual void AsyncRun();
        virtual bool TryAsyncStronglyConsistentRun();
        virtual void AsyncWeaklyConsistentRun();
        virtual optype op_type() {
                return m_op_type;
        }
};

class capmap_workload_generator {
private:
        Context*                                m_context;
        CAPMap*                                 m_map;
        uint32_t                                m_range;
        vector<workload_config>*                m_workload;
public:
        capmap_workload_generator(Context* context, CAPMap* map, uint32_t range, vector<workload_config>* workload) {
                this->m_context = context;
                this->m_map = map;
                this->m_range = range;
                this->m_workload = workload;
        }
        Txn** Gen();
};
