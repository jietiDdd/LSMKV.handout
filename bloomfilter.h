#pragma once

#include <cstdint>
#include "MurmurHash3.h"



// 布隆过滤器
class BloomFilter{
private:
    uint32_t *data; // 存放数组
    const int length = 2048; // 哈希数组大小,此时误报率较小
    const int hashNumber = 4; // 哈希函数个数为4

public:
    BloomFilter();

    ~BloomFilter();

    void insert(uint64_t key);

    bool search(uint64_t key);
};