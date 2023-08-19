#ifndef RECOVER
#define RECOVER

#include <memory>
#include <mutex>
#include <fstream>
#include <vector>
#include<filesystem>
#include "Page.h"
#include "Transaction.h"
#include "Data.h"

class DataItem;
class DataManager;

// 日志记录器
// 日志文件标准格式为：[XChecksum] [Log1] [Log2] ... [LogN] [BadTail]，其中XChecksum为后续所有日志计算的Checksum，int类型；BadTail 是在数据库崩溃时，没有来得及写完的日志数据，BadTail不一定存在。
// 每条正确日志的格式为：[Size] [Checksum] [Data]，其中Size为4字节int，标识Data长度；Checksum为4字节int，为每条日志的校验和
class Logger{
public:
    static std::shared_ptr<Logger> instance(); // 获取Logger的单例对象
    bool init(); // 初始化Logger

    void log(std::vector<char> data); // 提交一条日志
    std::vector<char> next(); // 获取下一条日志
    void reset(); // 重置position的位置

    ~Logger();
    Logger(const Logger&) = delete; // 禁用拷贝构造函数
    Logger& operator=(const Logger&) = delete; // 禁用赋值运算符
private:
    Logger() = default; // 禁用外部构造
    int calCheckSum(int checkSum, std::vector<char>& log); // 在校验和checkSum的基础上继续计算校验和
    void updateXChecksum(std::vector<char>& log); // 添加一条新日志后需要重新计算并更新校验和
    void checkAndRemoveTail(); // 检查log文件并移除bad tail
    std::vector<char> nextLog(); // 取下一条日志

    static const int seed=6160506; // 校验和计算种子
    static const int checkSumLength=sizeof(int); // 校验和长度
    static const int dataLength=sizeof(int); // 日志长度值的长度

    long long position; // 当前日志指针的位置
    int xChecksum; // 所有日志的校验和
    std::fstream file; // 日志文件
    std::mutex fileLock; // 文件访问互斥锁
};

class Recover {
public:
    struct UpdateLogInfo {
        long long xid;
        long long pageNumber;
        short offset;
        std::vector<char> oldData;
        std::vector<char> newData;
    };
    struct InsertLogInfo {
        long long xid;
        long long pageNumber;
        short offset;
        std::vector<char> data;
    };

    static void recover(); // 从日志中恢复
    static std::vector<char> updateLog(long long xid, DataItem& di); // 生成一条更新日志
    static std::vector<char> insertLog(long long xid, Page* page, std::vector<char>& raw); // 生成一条插入日志

private:
    Recover() = default; // 禁用外部构造
    static void redoTransactions(); // 重做事务
    static void undoTransactions(); // 撤销事务
    static UpdateLogInfo parseUpdateLog(std::vector<char>& log); // 解析更新日志
    static void doUpdateLog(std::vector<char>& log, int flag); // 执行更新日志
    static InsertLogInfo parseInsertLog(std::vector<char>& log); // 解析插入日志
    static void doInsertLog(std::vector<char>& log, int flag); // 执行插入日志

    static const char updateTypeLog = 0;
    static const char insertTypeLog = 1;
    static const int redo = 0;
    static const int undo = 1;
    // 更新日志的格式：[LogType] [XID] [UID] [OldRawLen] [OldRaw] [NewRaw]
    // 插入日志的格式：[LogType] [XID] [PageNumber] [Offset] [Raw]
    static const int typeLength=sizeof(char);
    static const int xidLength=sizeof(long long);
    static const int uidLength=sizeof(long long);
    static const int pageNumberLength=sizeof(long long);
    static const int offsetLength=sizeof(short);
    static const int oldRawLength=sizeof(short);
};

#endif