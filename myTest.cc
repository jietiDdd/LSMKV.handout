#include "kvstore.h"
#include <random>
#include <chrono>
#include <iostream>

#define SMALL_TEST 256
#define MID_TEST 1024 * 16
#define LARGE_TEST 1024 * 32
#define SMALL_SIZE 512
#define MID_SIZE 1024 * 8
#define LARGE_SIZE 1024 * 48
#define COMPACT_TEST 1024 * 6

// PUT测试
void putTest(std::vector<uint64_t> test, KVStore &store, uint64_t vlen){
    std::cout << "Put: " << std::endl;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(test.begin(), test.end(),gen);
    uint64_t size = test.size();

    auto start = std::chrono::high_resolution_clock::now();
    for(uint64_t i = 0; i < size; i++){
        store.put(test[i],std::string(vlen,'s'));
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double throughput = static_cast<double>(size) / latency * 1e9;
    auto average =  latency / size;

    std::cout << "Throughput: " << throughput << std::endl;
    std::cout << "Average latency: " << average << std::endl;
}

void getTest(std::vector<uint64_t> test, KVStore &store, uint64_t vlen){
    std::cout << "Get: " << std::endl;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(test.begin(), test.end(),gen);
    uint64_t size = test.size();

    auto start = std::chrono::high_resolution_clock::now();
    for(uint64_t i = 0; i < size; i++){
        store.get(test[i]);
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double throughput = static_cast<double>(size) / latency * 1e9;
    auto average =  latency / size;

    std::cout << "Throughput: " << throughput << std::endl;
    std::cout << "Average latency: " << average << std::endl;
}

void delTest(std::vector<uint64_t> test, KVStore &store, uint64_t vlen){
    std::cout << "Del: " << std::endl;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(test.begin(), test.end(),gen);
    uint64_t size = test.size();

    auto start = std::chrono::high_resolution_clock::now();
    for(uint64_t i = 0; i < size; i++){
        store.del(test[i]);
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double throughput = static_cast<double>(size) / latency * 1e9;
    auto average =  latency / size;

    std::cout << "Throughput: " << throughput << std::endl;
    std::cout << "Average latency: " << average << std::endl;
}


void normalTest(KVStore &store){
    std::cout << "Normal Test: " << std::endl;
    // 小测试，小到只会在内存中进行
    std::vector<uint64_t> smallTest;
    for(uint64_t i = 0; i < SMALL_TEST; i++){
        smallTest.push_back(i);
    }

    store.reset();
    std::cout << "Small Test: " << std::endl;
    putTest(smallTest, store, MID_SIZE);
    getTest(smallTest, store, MID_SIZE);
    delTest(smallTest, store, MID_SIZE);

    // 中测试
    std::vector<uint64_t> midTest;
    for(uint64_t i = 0; i < MID_TEST; i++){
        midTest.push_back(i);
    }

    store.reset();
    std::cout << "Mid Test: " << std::endl;
    std::cout << "Small vlen: " << std::endl;
    putTest(midTest, store, SMALL_SIZE);
    getTest(midTest, store, SMALL_SIZE);
    delTest(midTest, store, SMALL_SIZE);

    store.reset();
    std::cout << "Mid vlen: " << std::endl;
    putTest(midTest, store, MID_SIZE);
    getTest(midTest, store, MID_SIZE);
    delTest(midTest, store, MID_SIZE);

    store.reset();
    std::cout << "Large vlen: " << std::endl;
    putTest(midTest, store, LARGE_SIZE);
    getTest(midTest, store, LARGE_SIZE);
    delTest(midTest, store, LARGE_SIZE);

    // 大测试
    store.reset();
    std::vector<uint64_t> largeTest;
    for(uint64_t i = 0; i < LARGE_TEST; i++){
        largeTest.push_back(i);
    }

    store.reset();
    std::cout << "Large Test: " << std::endl;
    std::cout << "Small vlen: " << std::endl;
    putTest(largeTest, store, SMALL_SIZE);
    getTest(largeTest, store, SMALL_SIZE);
    delTest(largeTest, store, SMALL_SIZE);

    store.reset();
    std::cout << "Mid vlen: " << std::endl;
    putTest(largeTest, store, MID_SIZE);
    getTest(largeTest, store, MID_SIZE);
    delTest(largeTest, store, MID_SIZE);

    store.reset();
    std::cout << "Large vlen: " << std::endl;
    putTest(largeTest, store, LARGE_SIZE);
    getTest(largeTest, store, LARGE_SIZE);
    delTest(largeTest, store, LARGE_SIZE);
}

// 这个测试需要手动更改
void cacheTest(KVStore &store){
    std::cout << "Cache Test: " << std::endl;
    store.reset();
    // 只进行大测试，不然都没有SSTable
    std::vector<uint64_t> largeTest;
    for(uint64_t i = 0; i < LARGE_TEST; i++){
        largeTest.push_back(i);
    }
    for(uint64_t i = 0; i < LARGE_TEST; i++){
        store.put(largeTest[i],std::string(MID_SIZE,'s'));
    }
    getTest(largeTest,store, MID_SIZE);
}

// 这里把数据放的比较小，因为全部时延都会打印
void compactTest(KVStore &store){
    std::cout << "Compact Test: " << std::endl;
    store.reset();
    std::vector<uint64_t> compactTest;
    
    for(uint64_t i = 0; i < COMPACT_TEST; i++){
        compactTest.push_back(i);
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(compactTest.begin(), compactTest.end(),gen);

    for(uint64_t i = 0; i < COMPACT_TEST; i++){
        auto start = std::chrono::high_resolution_clock::now();
        store.put(compactTest[i],std::string(compactTest[i],'s'));
        auto end = std::chrono::high_resolution_clock::now();
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::cout << latency << " ";
    }
}

// 也要手动更改，因为大小直接就定义为宏了，不过很好改就是了
void bloomTest(KVStore &store){
    // 也是只进行大测试
    std::vector<uint64_t> largeTest;
    for(uint64_t i = 0; i < LARGE_TEST; i++){
        largeTest.push_back(i);
    }
    std::cout << "Bloom Test: " << std::endl;
    store.reset();
    putTest(largeTest, store, MID_SIZE);
    getTest(largeTest, store, MID_SIZE);
}

// 测试程序
int main() {
    KVStore store("./data", "./data/vlog");
    // normalTest(store);
    cacheTest(store);
    // compactTest(store);
    // bloomTest(store);
}