#pragma once

#include <capmap.h>
#include <request.h>
#include <capmap_config.h>

class ycsb_cross_insert : public Txn {
private:
        CAPMap*                                 m_map;
        uint64_t                                m_start;
        uint64_t                                m_end;
        ColorSpec                               m_color;
        Context*                                m_context;
        optype                                  m_op_type;
public:
        ycsb_cross_insert(CAPMap* map, ColorSpec color, uint64_t start, uint64_t end, Context* context, optype op_type) {
                this->m_map = map;
                this->m_color = color;
                this->m_start = start;
                this->m_end = end;
                this->m_context = context;
                this->m_op_type = op_type;
        }
        ~ycsb_cross_insert(){}
        virtual void Run();
        virtual WriteId AsyncRun();
        virtual WriteId AsyncRemoteRun();
        virtual WriteId AsyncStronglyConsistentRun();
        virtual WriteId AsyncWeaklyConsistentRun();
        WriteId AsyncPartitioningAppend();
        WriteId AsyncHealingAppend();
        virtual optype op_type() {
                return m_op_type;
        }
};

class ycsb_cross_read : public Txn {
private:
        CAPMap*                                 m_map;
        uint64_t                                m_start;
        uint64_t                                m_end;
        ColorSpec                               m_primary_color;
        Context*                                m_context;
        optype                                  m_op_type;
public:
        ycsb_cross_read(CAPMap* map, ColorSpec primary_color, uint64_t start, uint64_t end, Context* context, optype op_type) {
                this->m_map = map;
                this->m_primary_color = primary_color;
                this->m_start = start;
                this->m_end = end;
                this->m_context = context;
                this->m_op_type = op_type;
        }
        ~ycsb_cross_read(){}
        virtual void Run();
        virtual WriteId AsyncRun();
        virtual WriteId AsyncRemoteRun();
        virtual WriteId AsyncStronglyConsistentRun();
        virtual WriteId AsyncWeaklyConsistentRun();
        virtual optype op_type() {
                return m_op_type;
        }
};

class capmap_workload_generator {
private:
        Context*                                m_context;
        CAPMap*                                 m_map;
        uint64_t                                m_range;
        vector<workload_config>*                m_workload;
public:
        capmap_workload_generator(Context* context, CAPMap* map, uint64_t range, vector<workload_config>* workload) {
                this->m_context = context;
                this->m_map = map;
                this->m_range = range;
                this->m_workload = workload;
        }
        Txn** Gen();
};
