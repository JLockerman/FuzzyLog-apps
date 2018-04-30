#include <or_set.h>
#include <cassert>
#include <iostream>
#include <vector>

or_set::or_set(DAGHandle *handle, struct colors *color, struct colors *remote_colors, uint8_t proc_id,
	       uint64_t sync_duration)
{
	_log_client = handle;
	_color = color;
	_remote_colors = remote_colors;
	_proc_id = proc_id;
	_guid_counter = 0;
	_state.clear();
	_sync_duration = std::chrono::microseconds(sync_duration);

	snapshot(_log_client);
	/* Turn off anti-entropy for the time being */
	//	_sync_thread = 	std::thread(&or_set::check_remotes, this);
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

bool or_set::get_single_remote()
{
	size_t buf_sz;
	struct colors c;
	uint64_t *uint_buf;

	get_next(_log_client, _buf, &buf_sz, &c);

	if (c.numcolors == 0) {
		snapshot(_log_client);
		return false;
	}

	uint_buf = reinterpret_cast<uint64_t*>(_buf);
	auto opcode = uint_buf[0];

	switch (get_opcode(opcode)) {
	case ADD:
		remote_add((uint8_t*)_buf, buf_sz);
		break;
	case REMOVE:
		remote_remove((uint8_t*)_buf, buf_sz);
		break;
	case TRANSACTION:
		remote_transaction((uint8_t*)_buf, buf_sz);
		break;
	default:
		/* XXX Debugging log playback issue. */
		std::cerr << std::showbase << std::hex;
		std::cerr << opcode << "\n";
		assert(false);
	}
	return true;

	/*
	size_t data_sz, locs_read;
	auto entry = get_next2(_log_client, &data_sz, &locs_read);

	if (locs_read == 0) {
		snapshot_colors(_log_client, _remote_colors);
		return false;
	}

	auto uint_buf = reinterpret_cast<const uint64_t*>(entry.data);
	auto opcode = uint_buf[0];
	assert(get_proc_id(opcode) != _proc_id);
	switch (get_opcode(opcode)) {
	case ADD:
		remote_add(entry.data, data_sz);
		break;
	case REMOVE:
		remote_remove(entry.data, data_sz);
		break;
	default:
		std::cerr << std::showbase << std::hex;
		std::cerr << opcode << "\n";
		assert(false);
	}
	assert(false);
	return true;
	*/
}


void or_set::get_remote_updates()
{
	size_t buf_sz;
	struct colors c;
	uint64_t *uint_buf;

	/* XXX Shouldn't get here! */
	assert(false);
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
	uint64_t *rbuf;

	if (*buf == NULL) {
		rbuf = static_cast<uint64_t*>(malloc(sizeof(uint64_t)*3));
		*buf = reinterpret_cast<char*>(rbuf);
	} else {
		rbuf = reinterpret_cast<uint64_t*>(*buf);
	}

	rbuf[0] = make_opcode(ADD);
	rbuf[1] = e;
	rbuf[2] = guid;

	*sz = sizeof(uint64_t)*3;
	return;
}

void or_set::send_add(uint64_t e, uint64_t guid)
{
	char *buf;
	size_t sz;

	buf = NULL;
	serialize_add(e, guid, &buf, &sz);
	append(_log_client, buf, sz, _color, NULL);
}

write_id or_set::send_add_async(uint64_t e, uint64_t guid, char *buf)
{
	size_t sz;
	serialize_add(e, guid, &buf, &sz);
//	std::cerr << "Serialized request!\n";
//	std::cerr << "Request size: " << sz << "\n";
	return async_append(_log_client, buf, sz, _color, NULL);
}


void or_set::serialize_remove(uint64_t e, const std::set<uint64_t> &guid_set, char **buf, size_t *sz)
{
	assert(guid_set.size() > 0);

	uint64_t *rbuf;

	if (*buf == NULL) {
		rbuf = static_cast<uint64_t*>(malloc(sizeof(uint64_t)*2 + sizeof(uint64_t)*guid_set.size()));
		*buf = reinterpret_cast<char*>(rbuf);
	} else {
		rbuf = reinterpret_cast<uint64_t*>(*buf);
	}
	rbuf[0] = make_opcode(REMOVE);

	rbuf[1] = e;

	auto i = 2;
	for (auto guid : guid_set) {
		rbuf[i] = guid;
		i += 1;
	}

	*sz = sizeof(uint64_t)*(2 + guid_set.size());
}

void or_set::send_remove(uint64_t e, const std::set<uint64_t> &guid_set)
{
	char *buf;
	size_t sz;

	buf = NULL;
	serialize_remove(e, guid_set, &buf, &sz);
	append(_log_client, buf, sz, _color, NULL);
}

write_id or_set::send_remove_async(uint64_t e, const std::set<uint64_t> &guid_set, char *buf)
{
	assert(buf != NULL);
	size_t sz;

	serialize_remove(e, guid_set, &buf, &sz);
	return async_append(_log_client, buf, sz, _color, NULL);
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

void or_set::remote_remove(const uint8_t *buf, size_t sz)
{
	uint64_t e;
	std::set<uint64_t> guids;

	deserialize_remove(buf, sz, &e, &guids);
	do_remove(e, guids);
}

void or_set::remote_add(const uint8_t *buf, size_t sz)
{
	uint64_t e, guid;

	deserialize_add(buf, sz, &e, &guid);
	do_add(e, guid);
}

void or_set::deserialize_remove(const uint8_t *buf, size_t sz, uint64_t *e, std::set<uint64_t> *guids)
{
	auto uint_buf = reinterpret_cast<const uint64_t*>(buf);
	auto opcode = get_opcode(uint_buf[0]);
	assert(opcode == REMOVE);

	*e = uint_buf[1];

	auto num_guids = sz / sizeof(uint64_t);
	for (auto i = 2; i < num_guids; ++i)
		guids->insert(uint_buf[i]);
}

void or_set::deserialize_add(const uint8_t *buf, size_t sz, uint64_t *e, uint64_t *guid)
{
	auto uint_buf = reinterpret_cast<const uint64_t*>(buf);
	auto opcode = get_opcode(uint_buf[0]);
	assert(opcode == ADD && sz == sizeof(uint64_t)*3);

	*e = uint_buf[1];
	*guid = uint_buf[2];
}

void or_set::remote_transaction(const uint8_t *buf, size_t sz)
{
	auto uint_buf = reinterpret_cast<const uint64_t*>(buf);
	auto opcode = get_opcode(uint_buf[0]);
	assert(opcode == TRANSACTION);
	auto elems = uint_buf[1];
	for(size_t i = 2; i < elems;) {
		switch (uint_buf[i]) {
		case ADD: {
			auto to_add = uint_buf[i++];
			auto guid = uint_buf[i++];
			do_add(to_add, guid);
			break;
		}
		break;
		case REMOVE: {
			std::set<uint64_t> guid_set;
			auto to_remove = uint_buf[i++];
			auto size = uint_buf[i++];
			for (int j = 0; j < size; j++) guid_set.insert(uint_buf[i++]);
			do_remove(to_remove, guid_set);
			break;
		}
		break;
		default: {
			/* XXX Debugging log playback issue. */
			std::cerr << std::showbase << std::hex;
			std::cerr << uint_buf[i] << "\n";
			assert(false);
			return;
		}}
	}
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

write_id or_set::async_add(uint64_t e, char *buf)
{
	{
		std::lock_guard<std::mutex> lck(_instance_mutex);
		auto guid = gen_guid();
		do_add(e, guid);
		return send_add_async(e, guid, buf);
	}
}

write_id or_set::async_remove(uint64_t e, char *buf)
{
	{
		std::lock_guard<std::mutex> lck(_instance_mutex);
		if (_state.count(e) == 0)
			return WRITE_ID_NIL;

		auto guid_set = _state[e];
		do_remove(e, guid_set);
		return send_remove_async(e, guid_set, buf);
	}
}

write_id or_set::async_transaction(uint64_t* es, bool *remove, size_t elems) {

	std::vector<uint64_t> buf;
	buf.push_back(make_opcode(TRANSACTION));
	buf.push_back((uint64_t)elems);

	{
		std::lock_guard<std::mutex> lck(_instance_mutex);
		for(size_t i = 0; i < elems; i++) {
			auto e = es[i];
			if (*remove) {
				if (_state.count(e) == 0) continue;
				auto guid_set = _state[e];
				do_remove(e, guid_set);

				buf.push_back(make_opcode(REMOVE));
				buf.push_back(e);
				buf.push_back(guid_set.size());
				for (auto guid : guid_set) {
					buf.push_back(guid);
				}
			} else {
				auto guid = gen_guid();
				do_add(e, guid);

				buf.push_back(make_opcode(ADD));
				buf.push_back(e);
				buf.push_back(guid);
			}
		}
		if (buf.size() == 1) return WRITE_ID_NIL;

		return async_append(
			_log_client,
			reinterpret_cast<char*>(buf.data()),
			buf.size() * sizeof(uint64_t),
			_color, //FIXME
			NULL
		);
	}
}
