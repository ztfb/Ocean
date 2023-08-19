#ifndef DATAITEM
#define DATAITEM

#include <vector>
#include <mutex>
#include "Page.h"
#include "Recover.h"
#include "Version.h"

class Logger;
class Recover;
class Entry;

class DataManager;
class VersionManager;
// DataItem 结构：[ValidFlag] [DataSize] [Data]，其中ValidFlag 1字节，0为有效，1为无效；DataSize  2字节，标识Data的长度
class DataItem {
public:
    friend class DataManager;
    friend class Recover;
    friend class Entry;
    friend class VersionManager;
    DataItem(Page* page,std::vector<char>& dataItem,std::vector<char>& oldDataItem,long long uid);
    void setValid(bool valid); // 设置DataItem的有效位
    bool isValid(); // 判断DataItem是否有效（是否被删除）
    char* getData(); // 获取数据
    void before(); // 修改DataItem数据前要调用的方法
    void unBefore(); // 撤销修改需要调用的方法
    void after(long long xid); // 修改DataItem数据后要调用的方法
    static DataItem* parseDataItem(Page* page,short offset); // 从页面的offset处解析并构造DataItem
    static std::vector<char> construct(std::vector<char>& data); // 从真正的数据构造出DataItem要求的数据格式
private:
    static const int validFlagLen=sizeof(char); // 有效位长度
    static const int dataSizeLen=sizeof(short); // 数据位长度
    std::vector<char> oldDataItem; // 老数据
    std::vector<char> dataItem; // 存储的数据；1字节为有效位；2-3字节为长度位；之后的字节是真正承载的数据
    Page* page; // 数据所在的页面
    long long uid; // DataItem地址
    std::mutex readLock; // 读锁
    std::mutex writeLock; // 写锁
};

// DataItem的缓存
class DataManager{
public:
    static std::shared_ptr<DataManager> instance(); // 获取DataManager的单例对象
    void init(long long memory); // 初始化DataManager

    DataItem* read(long long uid); // 根据地址uid读取数据项
    long long insert(long xid,std::vector<char>& data); // 事务XID插入数据data，并返回插入的DataItem的uid

    ~DataManager();
    DataManager(const DataManager&) = delete; // 禁用拷贝构造函数
    DataManager& operator=(const DataManager&) = delete; // 禁用赋值运算符
private:
    DataManager() = default; // 禁用外部构造
    void initFirstPage(); // 在创建文件时初始化第一个页
    bool loadFirstPage(); // 在打开已有文件时时读入第一个页，并验证正确性
    void initPageIndex(); // 初始化pageIndex
    DataItem* get(long long uid); // 从缓存中获取一个数据项，如果不在缓存中则从PageCache中载入
    void release(long long uid); // 释放一个数据项，如果没有其他使用者引用该数据项，将其从缓存中移除
    DataItem* getForCache(long long uid); // 根据地址uid读取数据，并包裹成DataItem返回。当键值为key的资源不在缓存中时，资源的获取方式
    void releaseForCache(DataItem* di); // 当资源被逐出缓存时的写入行为

    std::unordered_map<long long,DataItem*> cache; // 键值到数据项的映射
    std::unordered_map<long long,int> references; // 键值到该数据项的引用的个数的映射
    std::unordered_map<long long,bool> getting; // 是否有其他进程正在获取该数据项
    long long maxItemNumber; // 缓存最大可缓存的数据项数
    long long count=0; // 缓存中当前包含的数据项个数

    std::mutex resourceLock; // 资源访问互斥锁
};

#endif