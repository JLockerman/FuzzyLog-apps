#include <workload.h>
#include <cassert>
#include <random>
#include <iostream>

bool ycsb_insert::Run(HashMap *map) {
        uint32_t key, value;
        assert(map != NULL);
       
        // key = (start, end)
        key = rand() % (end - start);
        value = rand();

        map->put(key, value, color);

        return true;
}

void Table::populate(HashMap *map) {
        uint32_t i;
        for (i = 0; i < record_count; ++i) {
                //map->put(i, 0);
        }
}

uint32_t Table::num_records() const {
        return this->record_count;
}

Txn** workload_generator::Gen() {
        uint32_t i;
        uint32_t total_operation_count;
        double percent_of_single;
        double p;

        // percent of single operations
        total_operation_count = this->single_operation_count + this->multi_operation_count;
        percent_of_single = (double)this->single_operation_count / total_operation_count;

        // make colors for each operation
        struct colors* single_color; 
        struct colors* multi_color;

        // create single color
        single_color = (struct colors*)malloc(sizeof(struct colors));
        single_color->numcolors = 1;
        single_color->mycolors = new ColorID[0];
        single_color->mycolors[0] = this->color_of_interest->at(0);

        // create multi color
        multi_color = (struct colors*)malloc(sizeof(struct colors));
        multi_color->numcolors = this->color_of_interest->size();
        multi_color->mycolors = new ColorID[multi_color->numcolors];
        for (i = 0; i < multi_color->numcolors; ++i)
                multi_color->mycolors[i] = this->color_of_interest->at(i);


        Txn **txns = (Txn**)malloc(sizeof(Txn*) * total_operation_count);
        i = 0;
        while (this->single_operation_count > 0 || this->multi_operation_count > 0) {
                // dice
                p = ((double) rand() / (RAND_MAX));

                if (p < percent_of_single && single_operation_count > 0) {
                        // create single color operation
                        txns[i] = new ycsb_insert(0, table->num_records(), single_color);
                        single_operation_count--;
                        i++;
                        
                } else if (p >= percent_of_single && multi_operation_count > 0) {
                        // create multi color operation
                        txns[i] = new ycsb_insert(0, table->num_records(), multi_color);
                        multi_operation_count--;
                        i++;
                }
        }

        return txns;
}

void workload_generator::colors_to_struct(vector<struct colors*>* out) {
        uint32_t i;
        struct colors* color; 

        for (i = 0; i < color_of_interest->size(); ++i) {
                color = (struct colors*)malloc(sizeof(struct colors));
                color->numcolors = 1;
                color->mycolors = new ColorID[0];
                color->mycolors[0] = this->color_of_interest->at(0);
                out->push_back(color);
        }
}
