#ifndef           WORKLOAD_H_ 
#define           WORKLOAD_H_ 

#include <hashmap.h>

using namespace std;

class Txn {
public:
        Txn() {}
        virtual bool Run(HashMap *map) = 0;
        virtual uint32_t num_writes() = 0;
};

class ycsb_insert : public Txn {
private:
        uint32_t start;
        uint32_t end;
        uint32_t write_count;
public:
        ycsb_insert(uint32_t start, uint32_t end, uint32_t write_count) {
                this->start = start;
                this->end = end;
                this->write_count = write_count;
        }
        virtual bool Run(HashMap *map);
        virtual uint32_t num_writes();
};

class Table {
private:
        uint32_t record_count;
public:
        Table(uint32_t record_count) {
                this->record_count = record_count;
        }
        void populate(HashMap *map);
        uint32_t num_records() const;
};

class workload_generator {
private:
        Table *table;
        uint32_t operation_count;
public:
        workload_generator(Table *table, uint32_t operation_count) {
                this->table = table;
                this->operation_count = operation_count;
        }
        Txn** Gen();
};

#endif            // WORKLOAD_H_ 
