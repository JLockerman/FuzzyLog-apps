#pragma once

#include <string>

extern "C" {
        #include "fuzzy_log.h"
}

const uint32_t DefaultWindowSize = 32;

struct workload_config {
        std::string                     op_type;
        struct colors                   first_color;
        struct colors                   second_color;
        bool                            has_dependency;
        bool                            is_strong;
        uint64_t                        op_count;
};
