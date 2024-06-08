#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
	currentTimeStamp = 1;
	sstable.dir_path = dir;
	VLog.path = vlog;
	Memtable = new Skiplist();
	// 进行相关的初始化
	sstable.diskToCache();
	VLog.setHeadAndTail();
}

KVStore::~KVStore()
{
	std::string file_path = sstable.putNewFile();
	Memtable->to_disk(file_path,VLog,sstable.cacheMap);
	delete Memtable;
	// 是否需要清除缓存
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if(!Memtable->put(key, s)){ // 超过16KB，压入硬盘
		std::string file_path = sstable.putNewFile();
		Memtable->to_disk(file_path, VLog, sstable.cacheMap);
		sstable.compaction(); // 进行合并
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
	if(sstable.get(key,value,VLog,offset)){
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
	// 先进行查找
	if(get(key) == ""){
		return false;
	}
	put(key,"~DELETED~");
	return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	delete Memtable;
	for(auto &sstableLevel : sstable.cacheMap){ // 遍历每层
		for(auto &sstable : sstableLevel.second){ // 遍历每层每个文件
			utils::rmfile(sstable.first);
		}
		// 删除整层目录
		std::string levelPath = sstable.dir_path + "/level-" + std::to_string(sstableLevel.first);
		utils::rmdir(levelPath);
		sstableLevel.second.clear();
	}
	sstable.cacheMap.clear(); // 清除缓存
	utils::rmfile(VLog.path);
	VLog.head = VLog.tail = 0;
	
	// 清除后应当重新初始化
	currentTimeStamp = 1;
	Memtable = new Skiplist();
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
	sstable.scan(key1,key2,map,timeStamp,VLog);
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
	uint64_t current = VLog.tail;
	std::fstream file;
	Entry * entry;
	while((current - VLog.tail) < chunk_size){
		file.open(VLog.path, std::fstream::in | std::fstream::binary);
		file.seekg(current, std::ios::beg);

		entry = new Entry();

		file.read(&entry->magic,sizeof(char));
		file.read(reinterpret_cast<char*>(&entry->checkNum),sizeof(uint16_t));
		file.read(reinterpret_cast<char*>(&entry->key),sizeof(uint64_t));
		file.read(reinterpret_cast<char*>(&entry->vlen), sizeof(uint32_t));
		char* buffer = new char[entry->vlen];
		file.read(buffer, entry->vlen);
		entry->value = std::string(buffer, entry->vlen);
		delete [] buffer;
		
		uint64_t tmp = current;

		current = file.tellg();
		file.close();

		uint64_t offset;
		std::string value;
		// 先在跳表中找，找到说明不是最新记录
		if(Memtable->get(entry->key,value)){
			// 不是最新记录
			delete entry;
			continue;
		}
		if(value == "~DELETED~"){
			// 也要跳
			delete entry;
			continue;
		}
		value = "";
		// 再在缓存中找，找到的offset对应上，就放回去，免得覆写了Memtable
		if(sstable.get(entry->key,value,VLog,offset)){
			if(offset == tmp){
				// 插入到Memtable中
				put(entry->key,entry->value);
			}
		}
		// 否则不处理
		delete entry;
	}

	// Memtable写入硬盘
	std::string file_path = sstable.putNewFile();
	Memtable->to_disk(file_path,VLog,sstable.cacheMap);
	delete Memtable;
	Memtable = new Skiplist();
	// 打空洞
	utils::de_alloc_file(VLog.path, VLog.tail, (current - VLog.tail));
	VLog.tail = current;
}