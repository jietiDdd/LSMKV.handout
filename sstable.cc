#include "sstable.h"

// 初始化level
SSTable::SSTable()
{
    levelFileNum[0] = 2;
}


// 在SSTable中查找，注意是在缓存中查找
bool SSTable::get(uint64_t key, std::string &value, vLog &vlog, uint64_t &offset)
{
    // 需要检查最新的记录，即需要检查时间戳
    uint64_t newTimeStamp = 0; // 记录最新的时间戳
    bool isFound = false;
    for(auto &levelDir : cacheMap){ // 遍历每一层
        for(auto &cachePair : levelDir.second){ // 遍历每一层的每一个CacheTable
            if(cachePair.second.timeStamp > newTimeStamp){ // 只检查最新的记录
                if(getByOne(cachePair.second, key, value, vlog, offset)){ // 找到了
                    newTimeStamp = cachePair.second.timeStamp;
                    isFound = true;
                } else {
                    // 没找到，检查是否将value设置为DELETED
                    if(value == "~DELETED~"){
                        value = "";
                        newTimeStamp = cachePair.second.timeStamp;
                        isFound = false;
                    }
                }
            }
        }
    }
    return isFound;
}

// 查找缓存中的单个SSTable
bool SSTable::getByOne(CacheTable cacheTable, uint64_t key, std::string &value, vLog &vlog, uint64_t &offset)
{
    
    // 遍历元组之前，应当先检查键的最值和布隆过滤器，提高效率
    // 先检查最值
    if(key < cacheTable.minKey || key > cacheTable.maxKey){
        return false;
    }
    // 再检查布隆过滤器
    if(!cacheTable.bloomFilter.search(key)){
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
                offset = cacheTable.offsetList[mid];
                value = vlog.get(cacheTable.offsetList[mid], vlenList[mid]);
                return true;
            }
            offset = cacheTable.offsetList[mid];
            value = "~DELETED~";
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
    for(uint64_t i = less; i <= max; i++){
        key = keyList[i];
        if(timeStamp.find(key) != timeStamp.end()){
            // 已有记录
            if(timeStamp[key] < thisTimeStamp){
                // 可以替换时间戳
                timeStamp[key] = thisTimeStamp;
                if(vlenList[i] != 0){
                    map[key] = vlog.get(offsetList[i],vlenList[i]);
                } else {
                    // 已被删除
                    map.erase(key);
                }
            }
        } else {
            // 没有记录，放心放入
            timeStamp[key] = thisTimeStamp;
            if(vlenList[i] != 0){
                map[key] = vlog.get(offsetList[i],vlenList[i]);
            }
        }
    }
}

/*
 * 合并操作（最难的）
 * 操作是硬盘写入和缓存写入同时进行，但只读缓存
 */
void SSTable::compaction()
{
    // 在PUT操作后都要调用合并操作，为此需要先检验要不要合并
    if(cacheMap[0].size() <= levelFileNum[0]){
        return; // 没有超出，返回
    }

    // 遍历level，进行合并
    for(uint32_t level = 0; cacheMap[level].size() > levelFileNum[level]; level++){ // 检验是否超出当前层的容量
        // 对于每一个level的操作，分解为四个函数处理
        // 1. 统计当前level需要合并的文件
        std::vector<std::pair<std::string, CacheTable>> selected; // 所有需要合并的文件
        std::uint64_t minKey; // 便于找下一层的交集文件
        std::uint64_t maxKey;
        std::uint64_t timeStamp; // 合并后这些文件的新时间戳
        select_overflow(cacheMap[level], selected, level, minKey, maxKey, timeStamp);

        // 2. 找到下一层中键区间有交集的文件
        uint32_t nextLevel = level + 1;
        if(levelFileNum.find(nextLevel) == levelFileNum.end()){ // 先看看下一层是否存在，不存在就创建
            levelFileNum[nextLevel] = 2 * levelFileNum[level];
        } 
        if(!utils::dirExists(dir_path + "/level-" + std::to_string(nextLevel))){
            utils::mkdir(dir_path + "/level-" + std::to_string(nextLevel));
        }
        select_next_level(cacheMap[nextLevel], selected, nextLevel,minKey, maxKey, timeStamp);

        // 2.5 删除原来的文件，并更新selected(毕竟路径无效了)
        std::vector<CacheTable> selectedSST;
        for(auto &cachePair : selected){
            utils::rmfile(cachePair.first);
            selectedSST.emplace_back(cachePair.second);
        }

        // 3. 使用归并排序
        uint64_t assignment = selectedSST.size() * MAX_KEY_NUMBER;
        std::vector<uint64_t> keyList; // 存放结果
        std::vector<uint64_t> offsetList;
        std::vector<uint32_t> vlenList;


        merge(keyList, offsetList, vlenList, selectedSST);

        // 4. 将结果切分后放入新的文件
        set_sstable(timeStamp, keyList, offsetList, vlenList, nextLevel);
    }
}

/**
 * 找到当前层的多余文件
 * 第一优先为时间戳小，第二优先为最小键小
*/
void SSTable::select_overflow(std::map<std::string, CacheTable> cacheList, std::vector<std::pair<std::string, CacheTable>> &selected, uint32_t level,
    uint64_t &minKey, uint64_t &maxKey, uint64_t &timeStamp)
{
    std::vector<std::pair<std::string, CacheTable>> temp(cacheList.begin(), cacheList.end());
    // 排序
    std::sort(temp.begin(), temp.end(), [](const auto &item1, const auto &item2){
        return (item1.second.timeStamp < item2.second.timeStamp)
            || ((item1.second.timeStamp == item2.second.timeStamp)
            && (item1.second.minKey < item2.second.minKey));
    });
    // 计算选中的数量
    int selectedNum = (level == 0) ? cacheList.size() : (cacheList.size() - levelFileNum[level]);
    minKey = UINT64_MAX;
    maxKey = 0;
    timeStamp = 0;
    // 压入到vector中
    // 计算selected的minKey和maxKey
    for(int i = 0; i < selectedNum; i++){
        selected.emplace_back(temp[i]);
        cacheMap[level].erase(temp[i].first);
        if(temp[i].second.minKey < minKey) minKey = temp[i].second.minKey;
        if(temp[i].second.maxKey > maxKey) maxKey = temp[i].second.maxKey;
        if(temp[i].second.timeStamp > timeStamp) timeStamp = temp[i].second.timeStamp;
    }
}

/**
 * 找到下一层中有键交集的文件
 * 注意这里的level要提前加一
*/
void SSTable::select_next_level(std::map<std::string, CacheTable> cacheList, std::vector<std::pair<std::string, CacheTable>> &selected, uint32_t level,
    uint64_t minKey, uint64_t maxKey, uint64_t &timeStamp)
{
    /* 应当提前检查
    if(levelFileNum.find(level) == levelFileNum.end()){ // 没有创建
        levelFileNum[level] = 2 * (level + 1);
        utils::mkdir(dir_path + "/level-" + std::to_string(level));
    }
    */
    for(auto & cachePair : cacheList){
        if(cachePair.second.minKey > maxKey || cachePair.second.maxKey < minKey){ // 没有交集
            continue;
        }
        // 有交集，压入vector
        selected.emplace_back(cachePair);
        cacheMap[level].erase(cachePair.first); // 在缓存中删了
        if(cachePair.second.timeStamp > timeStamp) timeStamp = cachePair.second.timeStamp;
    }
}

/**
 * 归并排序
 * 给一个有序序列的数组，依次进行归并排序
 * 返回一组vector
*/
void SSTable::merge(std::vector<uint64_t> &keyList, std::vector<uint64_t> &offsetList, std::vector<uint32_t> &vlenList,
    std::vector<CacheTable> &selected)
{
    uint64_t selectedSize = selected.size();
    // 保存每个cacheTable已经保存了多少key
    std::vector<uint64_t> finishKeys(selectedSize, 0);
    // 保存上一次的键和时间戳，便于覆写
    uint64_t oldMinKey = UINT64_MAX;
    uint64_t oldTimeStamp = 0;
    while(1){
        uint64_t minKey = UINT64_MAX;
        uint64_t offset;
        uint32_t vlen;
        uint64_t timeStamp;
        uint64_t minIt; // 持有最小值的索引
        // 遍历selected，找到最小值
        for(uint64_t i = 0; i < selectedSize; i++){
            if(finishKeys[i] < selected[i].KVNumber && selected[i].keyList[finishKeys[i]] < minKey){
                minKey = selected[i].keyList[finishKeys[i]];
                offset = selected[i].offsetList[finishKeys[i]];
                vlen = selected[i].vlenList[finishKeys[i]];
                minIt = i;
                timeStamp = selected[i].timeStamp;
            }
        }
        // 发现都没找到，说明遍历完成
        if(minKey == UINT64_MAX){
            break;
        }

        // 保存key++
        finishKeys[minIt] += 1;

        // 检查keylist尾部的元素，其小于等于minKey
        // 小于则直接push_back
        // 等于需要比较时间戳，大的才能保留
        if(keyList.size() > 0){
            if(oldMinKey == minKey){
                if(oldTimeStamp < timeStamp){
                    // 需要覆写
                    keyList.back() = minKey;
                    offsetList.back() = offset;
                    vlenList.back() = vlen;
                    // 更新时间戳
                    oldMinKey = minKey;
                    oldTimeStamp = timeStamp;
                }
                // 否则舍弃
            } else {
                // 也只会是小于，进行插入
                keyList.push_back(minKey);
                offsetList.push_back(offset);
                vlenList.push_back(vlen);
                oldMinKey = minKey;
                oldTimeStamp = timeStamp;
            }
        } else{
            // 第一次
            keyList.push_back(minKey);
            offsetList.push_back(offset);
            vlenList.push_back(vlen);
            oldMinKey = minKey;
            oldTimeStamp = timeStamp;
        }
    }

}

void SSTable::set_sstable(uint64_t timeStamp, std::vector<uint64_t> keyList, std::vector<uint64_t> offsetList, std::vector<uint32_t> vlenList,
    uint32_t level){
    uint64_t allKVNumber = keyList.size(); // 总的键值对数量
    uint64_t currentKVNumber; // 当前文件键值对数量
    while(allKVNumber > 0){
        if(allKVNumber <= MAX_KEY_NUMBER){ // 最后一个
            currentKVNumber = allKVNumber;
            allKVNumber = 0;
        } else {
            allKVNumber -= MAX_KEY_NUMBER;
            currentKVNumber = MAX_KEY_NUMBER;
        }
        // 乱写的，后续应当校验
        // 初始化缓存
        CacheTable cacheTable;
        cacheTable.timeStamp = timeStamp;
        cacheTable.KVNumber = currentKVNumber;
        cacheTable.minKey = keyList[0];
        cacheTable.maxKey = keyList[currentKVNumber - 1];
        cacheTable.keyList.assign(keyList.begin(), keyList.begin() + currentKVNumber);
        keyList.erase(keyList.begin(), keyList.begin() + currentKVNumber);
        cacheTable.offsetList.assign(offsetList.begin(), offsetList.begin() + currentKVNumber);
        offsetList.erase(offsetList.begin(), offsetList.begin() + currentKVNumber);
        cacheTable.vlenList.assign(vlenList.begin(), vlenList.begin() + currentKVNumber);
        vlenList.erase(vlenList.begin(), vlenList.begin() + currentKVNumber);
        // 计算布隆过滤器
        for(auto &it : cacheTable.keyList){
            cacheTable.bloomFilter.insert(it);
        }
        // 计算char数组
        char * bytes = new char[SSTABLE_MAX_BYTES];
        char * init = bytes;
        uint64_to_byte(cacheTable.timeStamp, &bytes);
        uint64_to_byte(cacheTable.KVNumber, &bytes);
        uint64_to_byte(cacheTable.minKey, &bytes);
        uint64_to_byte(cacheTable.maxKey, &bytes);
        cacheTable.bloomFilter.bloom_to_byte(&bytes);
        for(uint64_t i = 0; i < currentKVNumber; i++){
            uint64_to_byte(cacheTable.keyList[i], &bytes);
            uint64_to_byte(cacheTable.offsetList[i], &bytes);
            uint32_to_byte(cacheTable.vlenList[i], &bytes);
        }
        std::string path = dir_path + "/level-" + std::to_string(level) + "/" + std::to_string(cacheMap[level].size()) 
        + "-" + std::to_string(timeStamp) + "-" + std::to_string(currentTimeStamp) + ".sst";
        cacheMap[level][path] = cacheTable;

        // 写入硬盘
        std::fstream file;
        file.open(path, std::fstream::out | std::fstream::binary);
        file.write(init, HEAD_LENGTH + BLOOM_LENGTH + CELL_LENGTH * currentKVNumber);
        file.close();
        delete [] init;
    }
}

// 在Memtable溢出时生成新的文件
// 返回文件路径
std::string SSTable::putNewFile()
{
    std::string levelPath = dir_path + "/level-0";
    if(cacheMap[0].empty()){ // level-0为空，需要创建
        utils::mkdir(levelPath);
    }
    std::string filePath = levelPath + "/" + std::to_string(cacheMap[0].size()) + "-" + std::to_string(currentTimeStamp) + "-" + std::to_string(currentTimeStamp) +".sst";
    return filePath;
}

// 将硬盘中的内容放入到缓存中
// 即对缓存的初始化
void SSTable::diskToCache()
{
    // 这个函数并不会被reset调用，因此还承担找到最新时间戳的职责
    uint64_t getTimeStamp = 0;
    for(int i = 0; ; i++){
        // 查找各层目录
        std::string levelPath = dir_path + "/level-" + std::to_string(i);
        if(utils::dirExists(levelPath)){
            // 存在，遍历当前层下的文件
            std::vector<std::string> filePath;
            int fileNum = utils::scanDir(levelPath,filePath);
            for(;fileNum > 0; fileNum--){
                // 一次性读取完文件
                std::fstream file;
                file.open(levelPath + "/" + filePath[fileNum - 1], std::fstream::in | std::fstream::binary);
                file.seekg(0,std::ios::end);
                uint64_t fileSize = file.tellg();
                file.seekg(0, std::ios::beg);
                char *bytes = new char[fileSize];
                char *init = bytes;
                file.read(bytes, fileSize);
                file.close();
                // 进行初始化
                CacheTable cacheTable;
                cacheTable.timeStamp = byte_to_uint64(&bytes);
                if(cacheTable.timeStamp > getTimeStamp){
                    getTimeStamp = cacheTable.timeStamp;
                }
                cacheTable.KVNumber = byte_to_uint64(&bytes);
                cacheTable.minKey = byte_to_uint64(&bytes);
                cacheTable.maxKey = byte_to_uint64(&bytes);
                cacheTable.bloomFilter.byte_to_bloom(&bytes);
                for(uint64_t j = 0; j < cacheTable.KVNumber; j++){
                    cacheTable.keyList.push_back(byte_to_uint64(&bytes));
                    cacheTable.offsetList.push_back(byte_to_uint64(&bytes));
                    cacheTable.vlenList.push_back(byte_to_uint32(&bytes));
                }
                // 放入到缓存中
                cacheMap[i][filePath[fileNum - 1]] = cacheTable;
                delete [] init;
            }
            if(i > 0) levelFileNum[i] = 2 * levelFileNum[i - 1]; 
        }
        else{
            // 不存在该目录，停止缓存
            break;
        }
    }
    currentTimeStamp = getTimeStamp + 1;
}