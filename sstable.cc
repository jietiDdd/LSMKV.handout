#include "sstable.h"

// 在SSTable中查找，注意是在缓存中查找
bool SSTable::get(uint64_t key, std::string &value, vLog &vlog)
{
    // 需要检查最新的记录，即需要检查时间戳
    uint64_t newTimeStamp = 0; // 记录最新的时间戳
    bool isFound = false;
    for(auto &levelDir : cacheMap){ // 遍历每一层
        for(auto &cachePair : levelDir.second){ // 遍历每一层的每一个CacheTable
            if(cachePair.second.timeStamp > newTimeStamp){ // 只检查最新的记录
                if(getByOne(cachePair.second, key, value, vlog)){ // 找到了
                    newTimeStamp = cachePair.second.timeStamp;
                    isFound = true;
                }
            }
        }
    }
    return isFound;
}

// 查找缓存中的单个SSTable
bool SSTable::getByOne(CacheTable cacheTable, uint64_t key, std::string &value, vLog &vlog)
{
    
    // 遍历元组之前，应当先检查键的最值和布隆过滤器，提高效率
    // 先检查最值
    if(key < cacheTable.minKey || key > cacheTable.maxKey){
        return false;
    }
    // 再检查布隆过滤器
    if(!cacheTable.BloomFilter.search(key)){
        return false;
    }
    // 最后再考虑遍历，需要使用二分查找法
    std::vector<uint64_t> keyList = cacheTable.keyList;
    std::vector<uint32_t> vlenList = cacheTable.vlenList;
    uint64_t low = 0;
    uint64_t high = cacheTable.KVNumber - 1;
    uint64_t mid;
    while(low <= high){
        mid = (low + high) / 2;
        if(keyList[mid] == key){ // 找到键
            if(vlenList[mid] != 0){ // 且未被删除
                value = vlog.get(cacheTable.offsetList[mid], vlenList[mid]);
                return true;
            }
            return false;     
        }
        else if(keyList[mid] < key){ // 在右半部分
            low = mid + 1;
            continue;
        }
        else{
            high = mid - 1;
            continue;
        }
    }
    return false;
}

// 扫描介于k1与k2之间的键值对，为保证顺序，临时使用std::map
// 这里提供简单的方法，即直接查找所有缓存的SSTable
void SSTable::scan(uint64_t k1,uint64_t k2, 
        std::map<uint64_t, std::string> &map, std::map<uint64_t, uint64_t> &timeStamp, vLog &vlog)
{
    for(auto &levelDir : cacheMap){ // 遍历每一层
        for(auto &cachePair : levelDir.second){ // 遍历每一层的每一个CacheTable
            scanByOne(cachePair.second, k1, k2, map, timeStamp, vlog);
        }
    }
}

// 对单个SSTable进行扫描
// 注意时间戳的问题
void SSTable::scanByOne(CacheTable cacheTable, uint64_t k1, uint64_t k2, 
    std::map<uint64_t, std::string> &map, std::map<uint64_t, uint64_t> &timeStamp, vLog &vlog)
{
    if(k2 < cacheTable.minKey || k1 > cacheTable.maxKey){ // 不存在重叠区间
        return;
    }

    uint64_t less; // 下限索引
    uint64_t max; // 上限索引
    std::vector<uint64_t> keyList = cacheTable.keyList;
    std::vector<uint64_t> offsetList = cacheTable.offsetList;
    std::vector<uint32_t> vlenList = cacheTable.vlenList;

    // 使用二分查找确定索引区间
    if(k1 > cacheTable.minKey){
        uint64_t low = 0;
        uint64_t high = cacheTable.KVNumber - 1;
        uint64_t mid;
        while(low <= high){
            mid = (low + high) / 2;
            if(keyList[mid] == k1){ // 找到键
               less = mid;
               break;
            }
            else if(keyList[mid] < k1){ // 在右半部分
                low = mid + 1;
                continue;
            }
            else{
                less = mid; // 随时更新
                high = mid - 1;
                continue;
            }
        }
    } else {
        less = 0;
    }

    if(k2 < cacheTable.maxKey){
        uint64_t low = 0;
        uint64_t high = cacheTable.KVNumber - 1;
        uint64_t mid;
        while(low <= high){
            mid = (low + high) / 2;
            if(keyList[mid] == k2){ // 找到键
               max = mid;
               break;
            }
            else if(keyList[mid] < k2){ // 在右半部分
                max = mid; // 随时更新
                low = mid + 1;
                continue;
            }
            else{
                high = mid - 1;
                continue;
            }
        }
    } else {
        max = cacheTable.KVNumber - 1;
    }

    uint64_t key;
    uint64_t thisTimeStamp = cacheTable.timeStamp;
    // 扫描索引区间，注意更新时间戳
    for(int i = less; i <= max; i++){
        key = keyList[i];
        if(timeStamp.find(key) != timeStamp.end() && timeStamp.at(key) > thisTimeStamp){ // 原来的记录更加新，不必插入
            continue;
        }
        if(vlenList[i] != 0){ // 说明未删除，需要插入
            map.insert(std::make_pair(key, vlog.get(offsetList[i], vlenList[i])));
            timeStamp.insert(std::make_pair(key, thisTimeStamp));
            continue;
        }
        // 已删除，也在map中删去
        if(timeStamp.find(key) != timeStamp.end()){
            map.erase(key);
            timeStamp.erase(key);
        }
    }
}