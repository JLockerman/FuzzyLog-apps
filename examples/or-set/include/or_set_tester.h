#pragma once

#include <tester.h>
#include <unordered_map>
#include <utility>
#include <or_set.h>
#include <fuzzy_log.h>
#include <functional>

struct fuzzylog_buf {
	fuzzylog_buf 		*_next;
	char 			_buf[DELOS_MAX_DATA_SIZE];
};

struct or_set_rq : public tester_request {
	fuzzylog_buf 		*_buffer;
	or_set::log_opcode 	_opcode;	
	uint64_t 		_key;		
};

class wid_hasher {
public:
	std::size_t operator() (write_id const &wid) const {
			return (std::hash<uint64_t>{}(wid.p1)*71) ^ std::hash<uint64_t>{}(wid.p2);		
	}
};

class wid_equality {
public:
	bool operator() (write_id const &wid1, write_id const &wid2) const {
		return (wid1.p1 == wid2.p1) && (wid1.p2 == wid2.p2);
	}
};

class or_set_tester : public async_tester {
private:
	or_set 									*_or_set;	
	DAGHandle 								*_fuzzylog;
	std::unordered_map<write_id, or_set_rq*, wid_hasher, wid_equality> 	_request_map;
	fuzzylog_buf 								*_freelist;
	
protected:
	void issue_request(tester_request *rq);
	void wait_request(std::set<tester_request*> &done_set); 

public:
	or_set_tester(uint32_t window_sz, or_set *set, DAGHandle *fuzzylog);
};


