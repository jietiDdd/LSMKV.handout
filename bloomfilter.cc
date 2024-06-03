#include "bloomfilter.h"

// 构造函数
BloomFilter::BloomFilter()
{
    this->data.assign(length, 0);
}

// 在将Memtable转换为SSTable时，一一插入所有键，此后将BloomFilter存入硬盘
void BloomFilter::insert(uint64_t key)
{
    uint64_t hash[2] = {0}; // 产生一个128-bit的结果
    MurmurHash3_x64_128(&key,sizeof(key),1,hash);
    
    // 根据哈希值进行插入
    data[hash[0] % length] = 1;
    data[(hash[0] >> 32) % length] = 1;
    data[hash[1] % length] = 1;
    data[(hash[1] >> 32) % length] = 1;
}

// 快速判断SSTable中是否存在某一键值对，但可能误判
bool BloomFilter::search(uint64_t key)
{
    uint64_t hash[2] = {0}; // 产生一个128-bit的结果
    MurmurHash3_x64_128(&key,sizeof(key),1,hash);

    if(data[hash[0] % length] 
    && data[(hash[0] >> 32) % length] 
    && data[hash[1] % length] 
    && data[(hash[1] >> 32) % length]){
        return true;
    }
    return false;
}

// 将布隆过滤器变成一个字节数组
void BloomFilter::bloom_to_byte(char ** dst)
{
    for(int i = 0; i < length; i++){
        uint32_to_byte(data[i], dst);
    }
}

// 将字节数组变成布隆过滤器
void BloomFilter::byte_to_bloom(char **src)
{
    for(int i = 0; i < length; i++){
        data[i] = byte_to_uint32(src);
    }
}
