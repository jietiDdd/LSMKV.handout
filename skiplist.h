#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <cstdlib>
#include <list>
#include <utility>
#include <ctime>
#include "information.h"

int MAX_LEVEL = 15;
int MAX_KEY_NUMBER = 408;

//跳表节点类
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
    int keyNumber; // 跳表的键值对数目，便于检查Memtable是否大于16kB

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

    void put(uint64_t key, const std::string &value);

    bool get(uint64_t key, std::string &value);

    bool del(uint64_t key);

    std::list<std::pair<uint64_t,std::string>> scan(uint64_t k1,uint64_t k2);
};