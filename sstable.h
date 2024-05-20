#pragma once

#include "bloomfilter.h"
#include "global.h"
#include "vLog.h"

// 缓存的单个SSTable
struct CacheTable{
    uint64_t timeStamp; // 时间戳
    uint64_t KVNumber; // 键值对数目
    uint64_t minKey; // 键最小值
    uint64_t maxKey; // 键最大值
    BloomFilter BloomFilter; // 布隆过滤器
    std::vector<uint64_t> keyList; // 存放元组的键
    std::vector<uint64_t> offsetList; // 存放元组的偏移量
    std::vector<uint32_t> vlenList; // 存放元组的值长度
};

// 有关SSTable的相关处理，为了提高速度，提供缓存
class SSTable{
private:
    std::string directory; // 总目录
    
public:
    /*
     * 缓存的cacheMap，用于缓存所有SSTable
     * 形式为：
     * map<level,<path, cache>>
     */
    std::map<std::uint32_t, std::map<std::string, CacheTable>> cacheMap;

    SSTable();

    ~SSTable();

    bool get(uint64_t key, std::string &value, vLog &vlog);

    bool getByOne(CacheTable cacheTable, uint64_t key, std::string &value, vLog &vlog);

    void scan(uint64_t k1,uint64_t k2, 
        std::map<uint64_t, std::string> &map, std::map<uint64_t, uint64_t> &timeStamp, vLog &vlog);

    void scanByOne(CacheTable cacheTable, uint64_t k1, uint64_t k2, 
        std::map<uint64_t, std::string> &map, std::map<uint64_t, uint64_t> &timeStamp, vLog &vlog);

    // TODO:使用优先级队列进行扫描操作
    std::map<uint64_t, std::string> scanWithHeap(uint64_t k1,uint64_t k2, vLog &vlog){ };

};