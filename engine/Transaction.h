#ifndef TRANSACTION
#define TRANSACTION

#include <memory>
#include <mutex>
#include <fstream>
#include <unordered_map>

// 对一个事务的抽象
class Transaction{
public:
    static Transaction* newTransaction(long long xid,int level,std::unordered_map<long long,Transaction*> active);
    bool isInSnapshot(long long xid);

    long long xid; // 事务XID
    int level; // 事务隔离级别
    std::unordered_map<long long,bool> snapshot;
    bool autoAborted; // 是否是自动撤销
};

// 事务XID文件管理类
// 文件和数据在同一机器上，且不会移植时，无需考虑字节序的问题
class TransactionManager {
public:
    friend class Transaction;

    static std::shared_ptr<TransactionManager> instance(); // 获取TransactionManager的单例对象
    bool init(); // 初始化TransactionManager

    // 每一个事务有一个唯一的XID，每个事务的状态为三个状态之一：活跃（正在进行）、已提交、已撤销（回滚）
    long long begin(); // 开启一个事务，并返回事务的ID
    void commit(long long xid); // 提交一个事务
    void abort(long long xid); // 撤销一个事务
    bool isActive(long long xid); // 判断一个事务是否是活跃的
    bool isCommitted(long long xid); // 判断一个事务是否已提交
    bool isAborted(long long xid); // 判断一个事务是否已撤销

    ~TransactionManager();
    TransactionManager(const TransactionManager&) = delete; // 禁用拷贝构造函数
    TransactionManager& operator=(const TransactionManager&) = delete; // 禁用赋值运算符
private:
    TransactionManager() = default; // 禁用外部构造
    void updateXID(long long xid,char status); // 更新XID事务的状态
    bool checkXID(long long xid,char status); // 检测XID事务是否处于status状态
    std::fstream file; // xid文件
    std::mutex fileLock; // 文件互斥锁
    // 事务的三种状态
    static const char active = 0;
    static const char committed = 1;
    static const char aborted  = 2;
    static const long long supperXID = 0; // 超级事务的XID，超级事务状态固定为committed
    long long xidCounter=0; // 当前xid的个数
};

#endif