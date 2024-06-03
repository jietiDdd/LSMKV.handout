#include "global.h"

uint64_t currentTimeStamp; // 最新的时间戳

// 一些辅助函数，实现相关数据与二进制数据的相互转换，方便读写文件
void char_to_byte(char data, char **dst)
{
    char *src = (char *)(&data);
    **dst = *src;
    (*dst) += 1;
}

void uint16_to_byte(uint16_t data, char **dst)
{
    char *src = (char *)(&data);
    for(int i = 0; i < 2; i++){
        **dst = *(src + i);
        (*dst) += 1;
    }
}

void uint32_to_byte(uint32_t data, char **dst)
{
    char *src = (char *)(&data);
    for(int i = 0; i < 4; i++){
        **dst = *(src + i);
        (*dst) += 1;
    }
}

void uint64_to_byte(uint64_t data, char **dst)
{
    char *src = (char *)(&data);
    for(int i = 0; i < 8; i++){
        **dst = *(src + i);
        (*dst) += 1;
    }
}

void string_to_byte(std::string data, char **dst)
{
    const char *src = data.c_str();
    std::strcpy(*dst, src);
    (*dst) += data.length();
}

char byte_to_char(char ** src)
{
    char data;
    *((char *)(&data)) = **src;
    (*src) += 1;
    return data;
}

uint16_t byte_to_uint16(char ** src)
{
    uint16_t data;
    for(int i = 0; i < 2; i++){
        *((char *)(&data) + i) = **src;
        (*src) += 1;
    }
    return data;
}

uint32_t byte_to_uint32(char ** src)
{
    uint32_t data;
    for(int i = 0; i < 4; i++){
        *((char *)(&data) + i) = **src;
        (*src) += 1;
    }
    return data;
}

uint64_t byte_to_uint64(char ** src)
{
    uint64_t data;
    for(int i = 0; i < 8; i++){
        *((char *)(&data) + i) = **src;
        (*src) += 1;
    }
    return data;
}

std::string byte_to_string(char ** src, uint32_t length)
{
    std::string data(*src, length);
    (*src) += length;
    return data;
}
