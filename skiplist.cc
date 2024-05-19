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
std::list<std::pair<uint64_t,std::string>> Skiplist::scan(uint64_t k1,uint64_t k2)
{
    std::list<std::pair<uint64_t,std::string>> scanList;

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
            return scanList;
        }
    }while(p->key < k1);

    while(p->isData == true && p->key <= k2){
        if(p->value != "~DELETED"){
            // 未被标记为删除，放入list中
            scanList.push_back(std::make_pair(p->key,p->value));
        }
        p = p->forward[0];
    }
    return scanList;
}