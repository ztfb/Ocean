#ifndef VERSION
#define VERSION

#include <unordered_map>
#include <list>
#include <mutex>
#include "Data.h"
#include "Transaction.h"

class DataItem;
class DataManager;
class VersionManager;
// M向上层抽象出Entry；Entry结构：[XCRT] [XDEL] [data]。XCRT 是创建该条记录（版本）的事务编号，而 XDEL 则是删除该条记录（版本）的事务编号。
class Entry{
public:
    friend class VersionManager;
    static Entry* newEntry(DataItem* dataItem,long long uid); // 创建一个新的Entry
    static Entry* loadEntry(long long uid); // 加载一个Entry
    static std::vector<char> makeEntry(std::vector<char>& data,long long xid); // 根据事务的XID和数据制作一个Entry
    char* getData();
    long long getXCRT();
    long long getXDEL();
    void setXDEL(long long xid);
    long long getUid();
private:
    long long uid; // Entry地址
    DataItem* dataItem;
    static const int xcrtLen=sizeof(long long);
    static const int xdelLen= sizeof(long long);
};

// 可见性判断
class Visibility{
public:
    static bool isVersionSkip(Transaction* t,Entry* entry);
    static bool isVisible(Transaction* t,Entry* entry);
    static bool readCommitted(Transaction* t,Entry* entry);
    static bool repeatableRead(Transaction* t,Entry* entry);
};

// 锁表，用来预防死锁（维护了一个依赖等待图，以进行死锁检测）
class LockTable{
public:
    static std::shared_ptr<LockTable> instance(); // 获取LockTable的单例对象

    std::mutex* add(long long xid, long long uid); // 添加依赖关系【事务XID占用UID】。不需要等待则返回null，否则返回锁对象；会造成死锁则抛出异常
    void remove(long long xid); // 移除一个事务

    LockTable(const LockTable&) = delete; // 禁用拷贝构造函数
    LockTable& operator=(const LockTable&) = delete; // 禁用赋值运算符
private:
    LockTable() = default; // 禁用外部构造
    void selectXID(long long uid); // 从等待队列中选择一个xid来占用uid
    bool hasDeadLock(); // 死锁检测
    bool dfs(long long xid,std::unordered_map<long long,int>& xidStamp,int& stamp);
    void removeFromList(std::unordered_map<long long, std::list<long long>>& listMap, long long uid0, long long uid1);
    void putIntoList(std::unordered_map<long long, std::list<long long>>& listMap, long uid0, long uid1);
    bool isInList(std::unordered_map<long long, std::list<long long>>& listMap, long uid0, long uid1);
    std::unordered_map<long long, std::list<long long>> x2u;  // 某个XID已经获得的资源的UID列表
    std::unordered_map<long long, long long> u2x;        // UID被某个XID持有
    std::unordered_map<long long, std::list<long long>> wait; // 正在等待UID的XID列表
    std::unordered_map<long long, std::mutex*> waitLock;   // 正在等待资源的XID的锁
    std::unordered_map<long long, long long> waitU;      // XID正在等待的UID
    std::mutex tableLock; // 表锁
};

// Entry的缓存
class VersionManager{
public:
    static std::shared_ptr<VersionManager> instance(); // 获取VersionManager的单例对象
    void init(); // 初始化VersionManager

    char* read(long long xid,long long uid);
    long long insert(long long xid,std::vector<char>& data);
    bool del(long long xid,long long uid);
    long long begin(int level);
    void commit(long long xid);
    void abort(long long xid);

    ~VersionManager();
    VersionManager(const VersionManager&) = delete; // 禁用拷贝构造函数
    VersionManager& operator=(const VersionManager&) = delete; // 禁用赋值运算符
private:
    VersionManager() = default; // 禁用外部构造
    Entry* get(long long uid); // 从缓存中获取一个实体
    void release(long long uid); // 释放一个实体，如果没有其他使用者引用该实体，将其从缓存中移除
    Entry* getForCache(long long uid); // 当键值为key的资源不在缓存中时，资源的获取方式
    void releaseForCache(Entry* entry); // 当资源被逐出缓存时的写入行为

    std::unordered_map<long long,Transaction*> activeTransaction; // 活跃的事务
    std::unordered_map<long long,Entry*> cache; // 键值到实体的映射
    std::unordered_map<long long,int> references; // 键值到该实体的引用的个数的映射
    std::unordered_map<long long,bool> getting; // 是否有其他进程正在获取该实体
    long long maxItemNumber; // 缓存最大可缓存的实体数
    long long count=0; // 缓存中当前包含的实体个数

    std::mutex resourceLock; // 资源访问互斥锁
    std::mutex transactionLock; // 事务操作锁
};

#endif
