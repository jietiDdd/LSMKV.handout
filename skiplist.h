#pragma once

#include "global.h"
#include "vlog.h"
#include "bloomfilter.h"
#include "sstable.h"

// 跳表节点类
struct Skiplist_Node{
    uint64_t key;
    std::string value;
    int level; // 级数从零开始
    std::vector<Skiplist_Node*> forward; // 每一级数的下一节点构成的指针数组
    bool isData; // 当结点为head或tail时，为false

    Skiplist_Node(uint64_t key,const std::string &value,int level,bool isData)
    {
        this->key = key;
        this->value = value;
        this->level = level;
        this->isData = isData;

        for(int i = 0; i <= level; i++){
            forward.push_back(NULL);
        }
    }

    ~Skiplist_Node(){}
};

// 跳表类
class Skiplist{
private:
    Skiplist_Node * head;
    Skiplist_Node * tail;
    int level; // 整个跳表的当前级数
    int keyNum; // 跳表的键值对数目，用于判断是否要转换为sstable和vlog

    // 内置函数，用于抛硬币，因此概率为0.5
    int random()
    {
        return rand() % 2;
    }

    // 内置函数，计算新节点级数
    int randomLevel()
    {
        int newLevel = 0;
        while(random() == 0 && newLevel < MAX_LEVEL){
            newLevel ++;
        }
        return newLevel;
    }

public:
    Skiplist();

    ~Skiplist();

    bool put(uint64_t key, const std::string &value);

    bool get(uint64_t key, std::string &value);

    bool del(uint64_t key);

    void scan(uint64_t k1,uint64_t k2,
        std::map<uint64_t, std::string> &map, std::map<uint64_t, uint64_t> &timeStamp);

    void to_disk(const std::string &file_path, vLog &vlog,
        std::map<std::uint32_t, std::map<std::string, CacheTable>> &cacheMap);
};