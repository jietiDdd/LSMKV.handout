#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <vector>
#include <cstdlib>
#include <list>
#include <utility>
#include <ctime>
#include <tuple>
#include <map>
#include <fstream>
#include <algorithm>
#include "utils.h"

// 有关跳表
#define MAX_LEVEL 15 // 跳表的最高阶数
#define MAX_KEY_NUMBER 408 // 跳表最多持有的键值对数，若超过就需要转换为SSTable

// 有关SSTable
#define SSTABLE_MAX_BYTES 16 * 1024 // SSTable的字节长度上限
extern uint64_t currentTimeStamp; // 最新的时间戳
#define HASH_LENGTH 2048 // 哈希数组大小
#define HEAD_LENGTH 32
#define BLOOM_LENGTH 8196
#define CELL_LENGTH 20

// 有关vLog
#define VLOG_ENTRY_HEAD 15 // entry除了value之外部分的字节数
#define VLOG_CHECK_HEAD 3 // entry在key之前的字节数

// 一些辅助函数，实现相关数据与二进制数据的相互转换，方便读写文件
void char_to_byte(char data, char **dst);

void uint16_to_byte(uint16_t data, char **dst);

void uint32_to_byte(uint32_t data, char **dst);

void uint64_to_byte(uint64_t data, char **dst);

void string_to_byte(std::string data, char **dst);

char byte_to_char(char ** src);

uint16_t byte_to_uint16(char ** src);

uint32_t byte_to_uint32(char ** src);

uint64_t byte_to_uint64(char ** src);

std::string byte_to_string(char ** src, uint32_t length);
