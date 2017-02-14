#include <or_set.h>
#include <cassert>
#include <iostream>

or_set::or_set(DAGHandle *handle, struct colors *color, uint8_t proc_id, 
	       uint64_t sync_duration)
{
	_log_client = handle;
	_color = color;
	_proc_id = proc_id;
	_guid_counter = 0;
	_state.clear();
	_sync_duration = std::chrono::microseconds(sync_duration);
	_sync_thread = 	std::thread(&or_set::check_remotes, this); 
}

uint64_t or_set::make_opcode(log_opcode opcode)
{
	return (static_cast<uint64_t>(opcode) << 32) | _proc_id;
}

or_set::log_opcode or_set::get_opcode(uint64_t code)
{
	return static_cast<or_set::log_opcode>(code >> 32);
}

uint8_t or_set::get_proc_id(uint64_t code)
{
	return static_cast<uint8_t>(0xFF & code);
}

void or_set::get_remote_updates()
{
	size_t buf_sz;
	struct colors c;
	uint64_t *uint_buf;
	
	snapshot(_log_client);
	while (true) {
		auto gn_ret = get_next(_log_client, _buf, &buf_sz, &c);
		assert(gn_ret == 0);		
	
		if (c.numcolors == 0)
			break;
		
		uint_buf = reinterpret_cast<uint64_t*>(_buf);
		auto opcode = uint_buf[0];			
	
		if (get_proc_id(opcode) == _proc_id)
			continue;

		switch (get_opcode(opcode)) {
		case ADD: 
			remote_add(_buf, buf_sz);
			break;
		case REMOVE:	
			remote_remove(_buf, buf_sz);
			break;
		default:
			/* XXX Debugging log playback issue. */
			std::cerr << std::showbase << std::hex;
			std::cerr << opcode << "\n";	
			assert(false);
		}
        }
}

void or_set::check_remotes()
{
	while (true) {
		{
			std::lock_guard<std::mutex> lck(_instance_mutex);
			get_remote_updates();
		}
		std::this_thread::sleep_for(_sync_duration);
	}
}

uint64_t or_set::gen_guid() 
{
	auto guid = _guid_counter;
	_guid_counter += 1;
	return (guid << 8 | _proc_id);
}

void or_set::serialize_add(uint64_t e, uint64_t guid, char **buf, size_t *sz)
{
	auto rbuf = static_cast<uint64_t*>(malloc(sizeof(uint64_t)*3));
		
	rbuf[0] = make_opcode(ADD);
	rbuf[1] = e;
	rbuf[2] = guid;
	
	*buf = reinterpret_cast<char*>(rbuf);
	*sz = sizeof(uint64_t)*3;
	return;		
}

void or_set::send_add(uint64_t e, uint64_t guid)
{
	char *buf;
	size_t sz;
	
	serialize_add(e, guid, &buf, &sz);
	append(_log_client, buf, sz, _color, NULL);
}

void or_set::serialize_remove(uint64_t e, const std::set<uint64_t> &guid_set, char **buf, size_t *sz)
{
	assert(guid_set.size() > 0);
	
	auto rbuf = static_cast<uint64_t*>(malloc(sizeof(uint64_t)*2 + sizeof(uint64_t)*guid_set.size()));
	rbuf[0] = make_opcode(REMOVE);
	rbuf[1] = e;

	auto i = 2;
	for (auto guid : guid_set) {
		rbuf[i] = guid;
		i += 1;
	}	

	*buf = reinterpret_cast<char*>(rbuf);
	*sz = sizeof(uint64_t)*(2 + guid_set.size());
}

void or_set::send_remove(uint64_t e, const std::set<uint64_t> &guid_set)
{
	char *buf;
	size_t sz;
	
	serialize_remove(e, guid_set, &buf, &sz);
	append(_log_client, buf, sz, _color, NULL);
}

void or_set::do_add(uint64_t e, uint64_t guid)
{
	_state[e].insert(guid);	
}

void or_set::do_remove(uint64_t e, const std::set<uint64_t> &guid_set)
{
	if (_state.count(e) > 0) {
		for (auto guid : guid_set) {
			_state[e].erase(guid);
		}
		
		if (_state[e].size() == 0)
			_state.erase(e);
	}	
}

void or_set::remote_remove(char *buf, size_t sz)
{
	uint64_t e;
	std::set<uint64_t> guids;
	
	deserialize_remove(buf, sz, &e, &guids);	
	do_remove(e, guids);
}

void or_set::remote_add(char *buf, size_t sz)
{
	uint64_t e, guid;
	
	deserialize_add(buf, sz, &e, &guid);
	do_add(e, guid);
}

void or_set::deserialize_remove(char *buf, size_t sz, uint64_t *e, std::set<uint64_t> *guids)
{
	auto uint_buf = reinterpret_cast<uint64_t*>(buf);
	auto opcode = get_opcode(uint_buf[0]);
	assert(opcode == REMOVE);
	
	*e = uint_buf[1];
	
	auto num_guids = sz / sizeof(uint64_t);
	for (auto i = 2; i < num_guids; ++i) 
		guids->insert(uint_buf[i]);				
}

void or_set::deserialize_add(char *buf, size_t sz, uint64_t *e, uint64_t *guid) 
{
	auto uint_buf = reinterpret_cast<uint64_t*>(buf);
	auto opcode = get_opcode(uint_buf[0]); 
	assert(opcode == ADD && sz == sizeof(uint64_t)*3);

	*e = uint_buf[1];
	*guid = uint_buf[2];
}

bool or_set::lookup(uint64_t e)
{
	{
		std::lock_guard<std::mutex> lck(_instance_mutex);		
		return _state.count(e) > 0;
	}
}

void or_set::add(uint64_t e)
{
	{
		std::lock_guard<std::mutex> lck(_instance_mutex);
		auto guid = gen_guid();
		do_add(e, guid);
		send_add(e, guid);	
	}
}

void or_set::remove(uint64_t e)
{
	{
		std::lock_guard<std::mutex> lck(_instance_mutex);
		auto guid_set = _state[e];
		do_remove(e, guid_set);
		send_remove(e, guid_set);
	}
}
