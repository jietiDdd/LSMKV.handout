#pragma once

#include "global.h"

// 展示单个vLog entry
struct Entry{
    char magic; // 开始符号，默认为0xff
    uint16_t checkNum; // 校验和
    uint64_t key; // 键
    uint32_t vlen; // 值长度
    std::string value; // 值
};

class vLog{
private:
    std::vector<Entry> cacheEntry; // 反正先写着
    uint64_t tail = 0; // tail指针相对于文件开头的偏移量，假使head指针就是文件结尾好了
    std::string path; // vLog文件的路径
public:
    std::vector<uint64_t> addNewEntrys(std::vector<Entry> entries, uint64_t length); // 实现memtable将数据写入vLog，返回偏移量
    
    std::string get(uint64_t offset, uint32_t vlen);
};