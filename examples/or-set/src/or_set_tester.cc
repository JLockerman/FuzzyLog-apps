#include <or_set_tester.h>
#include <iostream>
#include <cassert>

or_set_tester::or_set_tester(uint32_t window_sz, or_set *set, FLPtr fuzzylog)
	: async_tester(window_sz)
{
	_or_set = set;
	_fuzzylog = fuzzylog;
	_freelist = static_cast<fuzzylog_buf*>(malloc(sizeof(fuzzylog_buf)*MAX_ASYNC_REQUESTS));
	for (auto i = 0; i < MAX_ASYNC_REQUESTS; ++i)
		_freelist[i]._next = &_freelist[i+1];
	_freelist[MAX_ASYNC_REQUESTS-1]._next = NULL;
}

void or_set_tester::use_idle_cycles()
{
//	_or_set->get_single_remote();
}

void or_set_tester::issue_request(tester_request *rq)
{
	assert(_freelist != NULL);
	auto or_rq = static_cast<or_set_rq*>(rq);
	WriteId w_id;
	auto fuzzy_buf = _freelist;
	_freelist = fuzzy_buf->_next;
	or_rq->_buffer = fuzzy_buf;

	switch (or_rq->_opcode) {
	case or_set::log_opcode::ADD:
		w_id = _or_set->async_add(or_rq->_key, fuzzy_buf->_buf.data());
		break;
	case or_set::log_opcode::REMOVE:
		w_id = _or_set->async_remove(or_rq->_key, fuzzy_buf->_buf.data());
		break;
	default:
		assert(false);
	}

	if (wid_equality{}(w_id, WRITE_ID_NIL)) {
		assert(or_rq->_opcode == or_set::log_opcode::REMOVE);
		_done_requests.push_back(or_rq);
		return;
	} else {
		assert(_request_map.count(w_id) == 0);
		_request_map[w_id] = or_rq;
	}
}

tester_request* or_set_tester::wait_single_request()
{
	tester_request *rq;

	if (_done_requests.size() > 0) {
		rq = _done_requests.front();
		_done_requests.pop_front();
	} else {
		auto w_id = fuzzylog_wait_for_any_append(_fuzzylog);
		assert(_request_map.count(w_id) > 0);
		rq = _request_map[w_id];
		_request_map.erase(w_id);
	}

	auto or_rq = static_cast<or_set_rq*>(rq);
	auto buf = or_rq->_buffer;
	buf->_next = _freelist;
	_freelist = buf;
	or_rq->_buffer = NULL;
	return rq;
}

void or_set_tester::wait_requests(std::set<tester_request*> &done_set)
{
	while (_done_requests.size() > 0) {
		auto rq = _done_requests.front();
		_done_requests.pop_front();

		auto or_rq = static_cast<or_set_rq*>(rq);
		auto buf = or_rq->_buffer;
		buf->_next = _freelist;
		_freelist = buf;
		or_rq->_buffer = NULL;

		done_set.insert(rq);
	}

	while (true) {
		auto w_id = fuzzylog_try_wait_for_any_append(_fuzzylog);
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
