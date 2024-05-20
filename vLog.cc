#include "vLog.h"

// 实现memtable将数据写入vLog，并返回偏移量数组
std::vector<uint64_t> vLog::addNewEntrys(std::vector<Entry> entries, uint64_t length)
{
    std::fstream file;
    file.open(path, std::fstream::out | std::fstream::binary);
    file.seekp(0, std::ios::end); // head指针在文件结尾
    uint64_t head = file.tellp(); // 获取head指针
    char* bytes = new char[length + 1];
    char* init = bytes;
    std::vector<uint64_t> offset; // 偏移量
    // 遍历entries，将他们加入到bytes这一个缓存的char数组
    for(auto &it : entries){
        // 注意提供的entries是包含了删除项的(防止offset不对齐)
        // 如果出现删除项，直接跳过并设置offset为0
        if(it.vlen == 0){
            offset.push_back(0);
            continue;
        }
        offset.push_back(head + (bytes - init)); // 计算偏移量
        char_to_byte(it.magic, &bytes);
        uint16_to_byte(it.checkNum, &bytes);
        uint64_to_byte(it.key, &bytes);
        uint32_to_byte(it.vlen, &bytes);
        string_to_byte(it.value, &bytes);
    }
    file.write(bytes, length);
    file.close();
    delete [] bytes;
}

// 根据偏移量和值长度找到相应的值
std::string vLog::get(uint64_t offset, uint32_t vlen)
{
    std::fstream file;
    file.open(path, std::fstream::in | std::fstream::binary);
    file.seekg(offset + VLOG_ENTRY_HEAD, std::ios::beg); // 根据偏移量定位到对应的entry
    char value[vlen];
    file.read(value,vlen); // 根据值长度读取
    file.close();
    return std::string(value,vlen);
}