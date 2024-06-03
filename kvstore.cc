#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
	SSTable.dir_path = dir;
	vLog.path = vlog;
	Memtable = new Skiplist();
	// 进行相关的初始化
	SSTable.diskToCache();
	vLog.setHeadAndTail();
}

KVStore::~KVStore()
{
	Memtable->to_disk(SSTable.dir_path,vLog,SSTable.cacheMap);
	delete Memtable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if(!Memtable->put(key, s)){ // 超过16KB，压入硬盘
		std::string file_path = SSTable.putNewFile();
		Memtable->to_disk(file_path, vLog ,SSTable.cacheMap);
		SSTable.compaction(); // 进行合并
		delete Memtable;
		Memtable = new Skiplist();
		Memtable->put(key,s); // 重新插入
	}
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string value = "";
	if(Memtable->get(key,value)){
		// 查找成功，返回
		return value;
	}
	if(value == "~DELETED~"){
		// 已删除，不必查找SStable
		return "";
	}
	value = "";
	uint64_t offset;
	if(SSTable.get(key,value,vLog,offset)){
		// 在SSTable中查找
		return value;
	}
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	// 只需要在Memtable中删除
	return Memtable->del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	delete Memtable;
	for(auto &sstableLevel : SSTable.cacheMap){ // 遍历每层
		for(auto &sstable : sstableLevel.second){ // 遍历每层每个文件
			utils::rmfile(sstable.first.c_str());
		}
		// 删除整层目录
		std::string levelPath = SSTable.dir_path + "level-" + std::to_string(sstableLevel.first);
		utils::rmdir(levelPath.c_str());
		sstableLevel.second.clear();
	}
	SSTable.cacheMap.clear(); // 清除缓存
	utils::rmfile(vLog.path.c_str());
	vLog.head = vLog.tail = 0;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	// 维护两个map，方便自动递增放置
	std::map<uint64_t,std::string> map; // KVmap
	std::map<uint64_t,uint64_t> timeStamp; // 维护每个键的时间戳，最新的时间戳才正确
	// 先扫描Memtable，其时间戳最大
	Memtable->scan(key1,key2,map,timeStamp);
	// 再扫描硬盘
	SSTable.scan(key1,key2,map,timeStamp,vLog);
	// map->list
	for(auto & pair : map){
		list.push_back(pair);
	}
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
	std::fstream file;
	file.open(vLog.path, std::fstream::in | std::fstream::binary);
	// 依次扫描Entry
	uint64_t current = vLog.tail;
	while((current - vLog.tail) <= chunk_size){
		Entry *entry = new Entry();
		file.read(&entry->magic,sizeof(char));
		file.read(reinterpret_cast<char*>(&entry->checkNum),sizeof(uint16_t));
		file.read(reinterpret_cast<char*>(&entry->key),sizeof(uint64_t));
		uint64_t offset;
		std::string value;
		if(SSTable.get(entry->key,value,vLog,offset)){
			if(offset == current){
				// 偏移量正确
				file.read(reinterpret_cast<char*>(&entry->vlen), sizeof(uint32_t));
				file.read(reinterpret_cast<char*>(&entry->value), entry->vlen);
				// 插入到Memtable中
				Memtable->put(entry->key,entry->value);
			}
		}
		// 否则不处理
		current = file.tellg();
		delete entry;
	}
	// Memtable写入硬盘
	Memtable->to_disk(SSTable.dir_path,vLog,SSTable.cacheMap);
	// 打空洞
	utils::de_alloc_file(vLog.path,vLog.tail,current - vLog.tail);
	vLog.tail = current;
}