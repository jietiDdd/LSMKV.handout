#pragma once

#include "skiplist.h"
#include "bloomfilter.h"
#include <cstdint>
#include <tuple>
#include <vector>

// 用于存放缓存的SSTable
class SSTable{
private:
    uint64_t time; // 时间戳
    uint64_t KVNumber; // 键值对数目
    uint64_t min; // 键最小值
    uint64_t max; // 键最大值
    BloomFilter bloomfilter; // 布隆过滤器
    std::vector<std::tuple<uint64_t,uint64_t,uint32_t>> KOV; // 存放<Key,Offset,Vlen>元组

public:
    SSTable(Skiplist Memtable);

    ~SSTable();

};