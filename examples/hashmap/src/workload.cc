#include <workload.h>
#include <cassert>
#include <random>
#include <iostream>

void ycsb_insert::Run(HashMap *map) {
        uint32_t key, value;
        assert(map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();

        map->put(key, value, m_color);
}

void ycsb_insert::AsyncRun(HashMap *map) {
        uint32_t key, value;
        assert(map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();

        map->async_put(key, value, m_color);
}

Txn** workload_generator::Gen() {
        uint32_t i;
        uint32_t total_operation_count;
        double percent_of_single;
        double p;

        // percent of single operations
        total_operation_count = this->m_single_operation_count + this->m_multi_operation_count;
        percent_of_single = (double)this->m_single_operation_count / total_operation_count;

        // make colors for each operation
        struct colors* single_color; 
        struct colors* multi_color;

        // create single color
        single_color = (struct colors*)malloc(sizeof(struct colors));
        single_color->numcolors = 1;
        single_color->mycolors = new ColorID[0];
        single_color->mycolors[0] = this->m_color_of_interest->at(0);

        // create multi color
        multi_color = (struct colors*)malloc(sizeof(struct colors));
        multi_color->numcolors = this->m_color_of_interest->size();
        multi_color->mycolors = new ColorID[multi_color->numcolors];
        for (i = 0; i < multi_color->numcolors; ++i)
                multi_color->mycolors[i] = this->m_color_of_interest->at(i);


        Txn **txns = (Txn**)malloc(sizeof(Txn*) * total_operation_count);
        i = 0;
        while (this->m_single_operation_count > 0 || this->m_multi_operation_count > 0) {
                // dice
                p = ((double) rand() / (RAND_MAX));

                if (p < percent_of_single && m_single_operation_count > 0) {
                        // create single color operation
                        txns[i] = new ycsb_insert(0, m_range, single_color);
                        m_single_operation_count--;
                        i++;
                        
                } else if (p >= percent_of_single && m_multi_operation_count > 0) {
                        // create multi color operation
                        txns[i] = new ycsb_insert(0, m_range, multi_color);
                        m_multi_operation_count--;
                        i++;
                }
        }

        return txns;
}
