#pragma once

#include <cstdint>
#include "MurmurHash3.h"
#include "global.h"

// 布隆过滤器
class BloomFilter{
private:
    std::vector<uint32_t> data; // 存放数组，保证其为8192个字节
    const int length = HASH_LENGTH; // 哈希数组大小,此时误报率较小
    const int hashNumber = 4; // 哈希函数个数为4

public:
    BloomFilter();

    // 自定义赋值运算符重载函数
    BloomFilter& operator=(const BloomFilter& other) {
        data = other.data;
        return *this;
    }

    void insert(uint64_t key);

    bool search(uint64_t key);

    void bloom_to_byte(char ** dst);

    void byte_to_bloom(char **src);
};