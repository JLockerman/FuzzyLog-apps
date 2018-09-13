#pragma once

#include <string>

extern "C" {
        #include "fuzzylog_async_ext.h"
}

const uint32_t DefaultWindowSize = 32;

struct workload_config {
        std::string                     op_type;
        ColorSpec                       colors;
        bool                            has_dependency;
        bool                            is_strong;
        uint64_t                        op_count;
};
