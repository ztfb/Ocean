#include "Transaction.h"

Transaction* Transaction::newTransaction(long long xid,int level,std::unordered_map<long long,Transaction*> active){
    Transaction* t=new Transaction;
    t->xid=xid;
    t->level=level;
    if(level!= 0) {
        for(auto iter=active.begin();iter!=active.end();iter++){
            t->snapshot.insert(std::pair<long long,bool>(iter->first, true));
        }
    }
    return t;
}

bool Transaction::isInSnapshot(long long xid){
    if(xid==TransactionManager::supperXID)return false;
    return snapshot.find(xid)!=snapshot.end();
}

static std::shared_ptr<TransactionManager> transactionManager=nullptr;
static std::mutex mutex;

std::shared_ptr<TransactionManager> TransactionManager::instance(){
    // 懒汉模式
    // 使用双重检查保证线程安全
    if(transactionManager==nullptr){
        std::unique_lock<std::mutex> lock(mutex); // 访问临界区之前需要加锁
        if(transactionManager==nullptr){
            transactionManager=std::shared_ptr<TransactionManager>(new TransactionManager());
        }
    }
    return transactionManager;
}

bool TransactionManager::init() {
    if(!std::ifstream(".xid").good()){
        // XID文件不存在时，需要创建一个新文件
        std::ofstream temp;
        temp.open(".xid", std::ios::out|std::ios::binary);
        temp.write(reinterpret_cast<const char*>(&xidCounter),sizeof(long long));
        temp.close();
    }
    // XID文件存在，需要检查XID文件是否合法（读取XID文件头获得XID文件的理论长度，对比实际长度）
    // XID文件头长度固定为sizeof(long long)，表示当前XID的个数；每个事务的状态占用长度固定为sizeof(char)
    file.open(".xid", std::ios::in|std::ios::out|std::ios::binary);
    // 获取文件大小
    file.seekg(0,std::ios::end); //设置文件指针到文件流的尾部
    long long size = file.tellg(); //读取文件指针的位置
    if(size<sizeof(long long)) return false;
    // 从文件头获取xidCounter
    file.seekg(0);
    file.read((reinterpret_cast<char *>(&xidCounter)), sizeof(long long));
    if(size!=xidCounter+sizeof(long long)) return false;
    return true;
}

TransactionManager::~TransactionManager() {
    file.close();
}

void TransactionManager::updateXID(long long xid, char status) {
    long long offset=sizeof(long long)+(xid-1)*sizeof(char); // 根据xid计算该事务的状态值在XID文件中的位置
    std::unique_lock<std::mutex> lock(fileLock); // 访问文件需要加锁
    file.seekp(offset);
    file.write(&status,sizeof(char));
    file.flush();
}

long long TransactionManager::begin() {
    long long xid=xidCounter+1;
    updateXID(xid,active);
    std::unique_lock<std::mutex> lock(fileLock); // 访问文件需要加锁
    xidCounter++;
    file.seekp(0);
    file.write(reinterpret_cast<const char*>(&xidCounter),sizeof(long long));
    file.flush();
    return xid;
}

void TransactionManager::commit(long long xid) {
    updateXID(xid,committed);
}

void TransactionManager::abort(long long xid) {
    updateXID(xid,aborted);
}

bool TransactionManager::checkXID(long long xid,char status){
    long long offset=sizeof(long long)+(xid-1)*sizeof(char); // 根据xid计算该事务的状态值在XID文件中的位置
    char state;
    std::unique_lock<std::mutex> lock(fileLock); // 访问文件需要加锁
    file.seekg(offset);
    file.read(&state,sizeof(char));
    return state==status;
}

bool TransactionManager::isActive(long long xid) {
    if(xid==supperXID)return false;
    return checkXID(xid,active);
}

bool TransactionManager::isCommitted(long long xid) {
    if(xid==supperXID) return true;
    return checkXID(xid,committed);
}

bool TransactionManager::isAborted(long long xid) {
    if(xid==supperXID) return false;
    return checkXID(xid,aborted);
}