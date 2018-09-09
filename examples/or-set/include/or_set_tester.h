#pragma once

#include <tester.h>
#include <unordered_map>
#include <utility>
#include <or_set.h>
#include <functional>
#include <deque>

extern "C" {
	#include <fuzzylog.h>
	#include <fuzzylog_async_ext.h>
}

struct fuzzylog_buf {
	fuzzylog_buf 		*_next;
	std::vector<char> 	_buf;
};

struct or_set_rq : public tester_request {
	fuzzylog_buf 		*_buffer;
	or_set::log_opcode 	_opcode;
	uint64_t 		_key;
};

class wid_hasher {
public:
	std::size_t operator() (WriteId const &wid) const {
			return (std::hash<uint64_t>{}(*(uint64_t *)&wid.bytes)*71)
				^ std::hash<uint64_t>{}(*(uint64_t *)&wid.bytes[8]);
	}
};

class wid_equality {
public:
	bool operator() (WriteId const &wid1, WriteId const &wid2) const {
		auto eq = true;
		for(size_t i = 0; i < 16; i++) eq &= wid1.bytes[i] == wid2.bytes[i];
		return eq;
	}
};

class or_set_tester : public async_tester {
private:
	or_set 									*_or_set;
	FLPtr 								_fuzzylog;
	std::unordered_map<WriteId, or_set_rq*, wid_hasher, wid_equality> 	_request_map;
	std::deque<or_set_rq*> 							_done_requests;
	fuzzylog_buf 								*_freelist;

protected:
	void issue_request(tester_request *rq);
	void wait_requests(std::set<tester_request*> &done_set);
	virtual void use_idle_cycles();
	tester_request* wait_single_request();

public:
	or_set_tester(uint32_t window_sz, or_set *set, FLPtr fuzzylog);
};


