#include <or_set_tester.h>
#include <iostream>
#include <cassert>

or_set_tester::or_set_tester(uint32_t window_sz, or_set *set, DAGHandle *fuzzylog)
	: async_tester(window_sz)
{
	_or_set = set;
	_fuzzylog = fuzzylog;
	_freelist = static_cast<fuzzylog_buf*>(malloc(sizeof(fuzzylog_buf)*1024));
	for (auto i = 0; i < 1024; ++i) 
		_freelist[i]._next = &_freelist[i+1];
	_freelist[1024-1]._next = NULL;	
}

void or_set_tester::issue_request(tester_request *rq)
{
	assert(_freelist != NULL);
	auto or_rq = static_cast<or_set_rq*>(rq);
	write_id w_id;
	auto fuzzy_buf = _freelist;
	_freelist = fuzzy_buf->_next;	

//	std::cerr << "Issuing request!\n";
	switch (or_rq->_opcode) {
	case or_set::log_opcode::ADD:
		w_id = _or_set->async_add(or_rq->_key, fuzzy_buf->_buf);
		break;
	case or_set::log_opcode::REMOVE:
		w_id = _or_set->async_remove(or_rq->_key, fuzzy_buf->_buf);		
		break;
	default:
		assert(false);
	}
	assert(_request_map.count(w_id) == 0);
	or_rq->_buffer = fuzzy_buf;
	_request_map[w_id] = or_rq;
}

void or_set_tester::wait_requests(std::set<tester_request*> &done_set) 
{
	
	while (true) {
//		std::cerr << "In iter loop!\n";
		auto w_id = try_wait_for_any_append(_fuzzylog);
//		std::cerr << "wait returned\n";
//		std::cerr << w_id.p1 << " " << w_id.p2 << "\n";
//		std::cerr << wid_equality{}(w_id, WRITE_ID_NIL) << "\n";
		if (wid_equality{}(w_id, WRITE_ID_NIL))
			break;

		assert(_request_map.count(w_id) > 0);
		auto rq = _request_map[w_id];				
		auto or_rq = static_cast<or_set_rq*>(rq);	
		done_set.insert(rq);
		auto buf = or_rq->_buffer;
		buf->_next = _freelist;
		_freelist = buf;
		or_rq->_buffer = NULL;
		
		_request_map.erase(w_id);
	}
}
