#include "skiplist.h"

// 构造函数
Skiplist::Skiplist()
{
    srand(time(NULL)); // 设置随机数种子

    level = 0;
    head = new Skiplist_Node(0,"NULL",MAX_LEVEL,false);
    tail = new Skiplist_Node(UINT64_MAX,"NULL",MAX_LEVEL,false);
    for(int i = 0; i <= MAX_LEVEL; i++){
        head->forward[i] = tail;
    }
}

// 析构函数
Skiplist::~Skiplist()
{
    Skiplist_Node * p = head;
    Skiplist_Node * tmp;
    while(p != tail){
        tmp = p;
        p = p->forward[0];
        delete tmp;
    }
    delete tail;
}

// PUT操作
void Skiplist::put(uint64_t key, const std::string &value)
{
    std::vector<Skiplist_Node*> update;
    Skiplist_Node *p = head;

    // 从顶层开始查找插入位置
    for(int i = level; i >= 0; i--){
        // 下一节点不是tail且小于键，继续向右查找
        while(p->forward[i]->isData == true && p->forward[i]->key < key){
            p = p->forward[i];
        }
        // update放置key插入时左边的节点，便于更新
        update[i] = p;
    }

    /*
    TODO: 如果插入或覆盖后，Memtable大小超过限制，对硬盘进行操作后再插入
    */

    // key若存在，需要进行替换而非插入
    p = p->forward[0];
    if(p->key == key){
        p->value = value;
        return;
    }

    // key不存在，进行插入
    int newLevel = randomLevel();
    if(newLevel > level){
        // 跳表高度增加
        for(int i = level + 1; i <= newLevel; i++){
            update[i] = head;
        }
        level = newLevel;
    }
    Skiplist_Node * newNode = new Skiplist_Node(key,value,newLevel,true);
    for(int i = 0; i <= newLevel; i++){
        // 插入节点
        newNode->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = newNode;
    }
}

// GET操作
bool Skiplist::get(uint64_t key, std::string &value)
{
    // 这只针对于在Memtable中的查找
    Skiplist_Node * p = head;

    // 从最高一级向下查找
    for(int i = level; i >= 0; i--){
        // 在该层停在不大于key的最大位置
        while(p->forward[i]->isData == true && p->forward[i]->key <= key){
            p = p->forward[i];
        }
        // 查找成功
        if(p->key == key){
            // 当前节点未被标记为"~DELETED~"
            if(p->value != "~DELETED~"){
                value = p->value;
                return true;
            }
            // 当前节点已被删除
            else{
                return false;
            }
        }
    }

    // 查找失败
    return false;
}

// DEL操作
bool Skiplist::del(uint64_t key)
{
    Skiplist_Node * p = head;

    // 查找需要删除的位置
    for(int i = level; i >= 0; i--){
        // 下一节点不是tail且小于键，继续向右查找
        while(p->forward[i]->isData == true && p->forward[i]->key <= key){
            p = p->forward[i];
        }
        // 搜索到记录，将其记录为删除标记，返回true
        if(p->key == key){
            p->value = "~DELETED~";
            return true;
        }  
    }
    // 未查找到记录，返回false
    return false;  
}

// SCAN操作
// 要重构成std::map形式，并维护时间戳
void Skiplist::scan(uint64_t k1,uint64_t k2,
        std::map<uint64_t, std::string> &map, std::map<uint64_t, uint64_t> &timeStamp)
{

    // 查找不小于K1的最小键
    Skiplist_Node * p = head;
    for(int i = level; i >= 0; i--){
        // 查找直到最底层
        while(p->forward[i]->isData == true && p->forward[i]->key < k1){
            p = p->forward[i];
        }
    }

    // 落在最底层，查找不小于k1的最小键
    do{
        p = p->forward[0];
        if(p->isData == false){
            //已经到达tail，返回空链表
            return;
        }
    }while(p->key < k1);

    while(p->isData == true && p->key <= k2){
        if(p->value != "~DELETED"){
            // 未被标记为删除，放入list中
            map.insert(std::make_pair(p->key,p->value));
            timeStamp.insert(std::make_pair(p->key, currentTimeStamp)); // 使用当前的时间戳，保证跳表的才是最新纪录
        }
        p = p->forward[0];
    }
}

/*
 * 当Memtable大小即将溢出时，将memtable写入硬盘
 */
void Skiplist::to_disk(const std::string &file_path, vLog &vlog, 
    std::map<std::uint32_t, std::map<std::string, CacheTable>> &cacheMap)
{
    // 在进行SSTable硬盘写入的同时写入缓存，提高效率
    CacheTable cacheTable;
    // TODO:减少计算

    // 首先遍历memtable，将所有结果压入到一个entry数组中
    std::vector<Entry> entries;
    uint64_t length = 0; // 计算新插入vLog的总长度

    Skiplist_Node *p = head;
    while(p->forward[0]->isData != false){
        p = p->forward[0];
        Entry entry;
        cacheTable.keyList.push_back(p->key);
        // 检查是否已被删除，如果是，需要设置vlen为0
        if(p->value != "~DELETED"){
            cacheTable.vlenList.push_back(p->value.length());

            entry.key = p->key;
            entry.vlen = p->value.length();
            entry.value = p->value;
            entry.magic = 0xff;
            // 计算校验和
            std::vector<unsigned char> data;
            const unsigned char* keyBytes = reinterpret_cast<const unsigned char*>(&entry.key);
            data.insert(data.end(), keyBytes, keyBytes + sizeof(entry.key));

            const unsigned char* vlenBytes = reinterpret_cast<const unsigned char*>(&entry.vlen);
            data.insert(data.end(), vlenBytes, vlenBytes + sizeof(entry.vlen));

            const unsigned char* valueBytes = reinterpret_cast<const unsigned char*>(entry.value.data());
            data.insert(data.end(), valueBytes, valueBytes + entry.value.size());
            entry.checkNum = utils::crc16(data);

            length += (VLOG_ENTRY_HEAD + entry.vlen);
        } else {
            cacheTable.vlenList.push_back(0);
            entry.vlen = 0;
        }
        entries.push_back(entry);
    }
    // 将结果放入vLog中，并得到offsetList
    cacheTable.offsetList = vlog.addNewEntrys(entries,length);

    // 计算SSTable的头部
    currentTimeStamp++;
    cacheTable.timeStamp = currentTimeStamp;
    cacheTable.KVNumber = cacheTable.keyList.size();
    cacheTable.minKey = cacheTable.keyList.front();
    cacheTable.maxKey = cacheTable.keyList.back();

    // 生成布隆过滤器
    for(auto &it : cacheTable.keyList){ // 遍历keyList并放入布隆过滤器
        cacheTable.BloomFilter.insert(it);
    }

    // 计算char数组
    char * bytes = new char[SSTABLE_MAX_BYTES];
    uint64_to_byte(cacheTable.timeStamp, &bytes);
    uint64_to_byte(cacheTable.KVNumber, &bytes);
    uint64_to_byte(cacheTable.minKey, &bytes);
    uint64_to_byte(cacheTable.maxKey, &bytes);
    cacheTable.BloomFilter.bloom_to_byte(&bytes);
    for(int i = 0; i < keyNum; i++){
        uint64_to_byte(cacheTable.keyList[i], &bytes);
        uint64_to_byte(cacheTable.offsetList[i], &bytes);
        uint32_to_byte(cacheTable.vlenList[i], &bytes);
    }
    // 插入到缓存的level 0
    cacheMap[0].insert(std::make_pair(file_path, cacheTable));

    std::fstream file;
    file.open(file_path, std::fstream::out | std::fstream::binary);
    file.write(bytes,HEAD_LENGTH + BLOOM_LENGTH + CELL_LENGTH * keyNum);
    file.close();
    delete [] bytes;
}