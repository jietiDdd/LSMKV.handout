CXX = g++
CXXFLAGS = -std=c++20 -Wall -g

# 源文件列表
SOURCES = kvstore.cc skiplist.cc sstable.cc vlog.cc global.cc bloomfilter.cc correctness.cc persistence.cc myTest.cc
# 头文件列表
HEADERS = kvstore.h skiplist.h sstable.h vlog.h global.h bloomfilter.h
# 对应的目标文件列表
OBJECTS = $(SOURCES:.cc=.o)

# 默认目标
all: correctness persistence myTest

# 生成可执行文件 correctness
correctness: $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o correctness correctness.o kvstore.o skiplist.o sstable.o vlog.o global.o bloomfilter.o

# 生成可执行文件 persistence
persistence: $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o persistence persistence.o kvstore.o skiplist.o sstable.o vlog.o global.o bloomfilter.o

# 生成可执行文件 myTest
myTest: $(OBJECT)
	$(CXX) $(CXXFLAGS) -o myTest myTest.o kvstore.o skiplist.o sstable.o vlog.o global.o bloomfilter.o

# 通用规则：编译对应的目标文件
%.o: %.cc $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 清理生成的文件
clean:
	-rm -f correctness persistence myTest $(OBJECTS)