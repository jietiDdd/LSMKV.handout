#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "vlog.h"
#include "global.h"
#include "bloomfilter.h"
#include <iostream>

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
	Skiplist * Memtable;

	SSTable sstable;

	vLog VLog;
public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
};
