#pragma once

#include <string>

extern "C" {
        #include "fuzzy_log.h"
}

struct workload_config {
        std::string                     op_type;
        struct colors                   color;
        struct colors                   dep_color;
        bool                            has_dependency;
        bool                            is_strong;
        uint64_t                        op_count;
};
