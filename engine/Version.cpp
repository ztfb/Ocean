#include "Version.h"

Entry* Entry::newEntry(DataItem* dataItem,long long uid){
    Entry* entry=new Entry;
    entry->uid=uid;
    entry->dataItem=dataItem;
    return entry;
}

Entry* Entry::loadEntry(long long uid){
    DataItem* dataItem=DataManager::instance()->read(uid);
    return newEntry(dataItem,uid);
}

std::vector<char> Entry::makeEntry(std::vector<char>& data,long long xid){
    std::vector<char> entry(xcrtLen+xdelLen+data.size());
    char* p=reinterpret_cast<char*>(&xid);
    std::copy(p,p+xcrtLen,entry.begin());
    std::copy(data.begin(),data.end(),entry.begin()+xcrtLen+xdelLen);
}

char* Entry::getData(){
    return dataItem->getData()+xcrtLen+xdelLen;
}

long long Entry::getXCRT(){
    long long xid=0;
    dataItem->readLock.lock();
    std::copy(dataItem->getData(),dataItem->getData()+xcrtLen,reinterpret_cast<char*>(&xid));
    dataItem->readLock.unlock();
    return xid;
}

long long Entry::getXDEL(){
    long long xid=0;
    dataItem->readLock.lock();
    std::copy(dataItem->getData()+xcrtLen,dataItem->getData()+xcrtLen+xdelLen,reinterpret_cast<char*>(&xid));
    dataItem->readLock.unlock();
    return xid;
}

void Entry::setXDEL(long long xid){
    dataItem->before();
    char* p=reinterpret_cast<char*>(&xid);
    std::copy(p,p+xdelLen,dataItem->getData()+xcrtLen);
    dataItem->after(xid);
}

long long Entry::getUid(){
    return this->uid;
}

bool Visibility::isVersionSkip(Transaction* t,Entry* entry){
    if(t->level==0){
        return false;
    }else{
        return TransactionManager::instance()->isCommitted(entry->getXDEL())&&(entry->getXDEL()>t->xid||t->isInSnapshot(entry->getXDEL()));
    }
}

bool Visibility::isVisible(Transaction* t,Entry* entry){
    if(t->level==0) {
        return readCommitted(t, entry);
    }else{
        return repeatableRead(t, entry);
    }
}

bool Visibility::readCommitted(Transaction* t,Entry* entry){
    if(entry->getXCRT()==t->xid&&entry->getXDEL()==0)return true;
    if(TransactionManager::instance()->isCommitted(entry->getXCRT())) {
        if(entry->getXDEL()==0) return true;
        if(entry->getXDEL()!=t->xid) {
            if(!TransactionManager::instance()->isCommitted(entry->getXDEL())) {
                return true;
            }
        }
    }
    return false;
}

bool Visibility::repeatableRead(Transaction* t,Entry* entry){
    if(entry->getXCRT()==t->xid&&entry->getXDEL()==0) return true;
    if(TransactionManager::instance()->isCommitted(entry->getXCRT())&&entry->getXCRT()<t->xid&&!t->isInSnapshot(entry->getXCRT())){
        if(entry->getXDEL()==0) return true;
        if(entry->getXDEL()!=t->xid) {
            if(!TransactionManager::instance()->isCommitted(entry->getXDEL())||entry->getXDEL()>t->xid||t->isInSnapshot(entry->getXDEL())) {
                return true;
            }
        }
    }
    return false;
}

static std::shared_ptr<LockTable> lockTable=nullptr;
static std::mutex mutex;

std::shared_ptr<LockTable> LockTable::instance(){
    // 懒汉模式
    // 使用双重检查保证线程安全
    if(lockTable==nullptr){
        std::unique_lock<std::mutex> lock(mutex); // 访问临界区之前需要加锁
        if(lockTable==nullptr){
            lockTable=std::shared_ptr<LockTable>(new LockTable());
        }
    }
    return lockTable;
}

std::mutex* LockTable::add(long long xid, long long uid){
    tableLock.lock();
    if(isInList(x2u,xid,uid))return nullptr;
    if(u2x.find(uid)==u2x.end()){
        u2x.insert({uid,xid});
        putIntoList(x2u,xid,uid);
        return nullptr;
    }
    waitU.insert({xid,uid});
    putIntoList(wait,uid,xid);
    if(hasDeadLock()){
        waitU.erase(xid);
        removeFromList(wait,uid,xid);
        throw "dead lock";
    }
    std::mutex* lock=new std::mutex;
    lock->lock();
    waitLock.insert({xid,lock});

    tableLock.unlock();
    return lock;
}

void LockTable::remove(long long xid){
    tableLock.lock();
    auto iter=x2u.find(xid);
    if(iter!=x2u.end()){
        while(!iter->second.empty()){
            long long uid=iter->second.front();
            iter->second.pop_front();
            selectXID(uid);
        }
    }
    waitU.erase(xid);
    x2u.erase(xid);
    waitLock.erase(xid);
    tableLock.unlock();
}

void LockTable::selectXID(long long uid){
    u2x.erase(uid);
    auto iter=wait.find(uid);
    if(iter==wait.end())return;
    while (!iter->second.empty()){
        long long xid=iter->second.front();
        iter->second.pop_front();
        if(waitLock.find(xid)==waitLock.end())continue;
        else{
            u2x.insert({uid,xid});
            std::mutex* lock=waitLock.find(xid)->second;
            waitU.erase(xid);
            lock.unlock();
            delete lock;
            break;
        }
    }
    if(iter->second.empty())wait.erase(iter);
}

bool LockTable::hasDeadLock(){
    std::unordered_map<long long,int> xidStamp;
    int stamp = 1;
    for(auto iter=x2u.begin();iter!=x2u.end();iter++){
        auto s=xidStamp.find(iter->first);
        if(s!=xidStamp.end()&&s->second>0)continue;
        stamp++;
        if(dfs(iter->first,xidStamp,stamp))return true;
    }
    return false;
}

bool LockTable::dfs(long long xid,std::unordered_map<long long,int>& xidStamp,int& stamp){
    auto stp=xidStamp.find(xid);
    if(stp!=xidStamp.end()&&stp->second==stamp)return true;
    if(stp!=xidStamp.end()&&stp->second<stamp)return false;
    xidStamp.insert({xid,stamp});
    auto uid=waitU.find(xid);
    if(uid==waitU.end())return false;
    return dfs(u2x.find(uid->second)->second,xidStamp,stamp);
}

void LockTable::removeFromList(std::unordered_map<long long, std::list<long long>>& listMap, long long uid0, long long uid1){
    auto iter=listMap.find(uid0);
    if(iter==listMap.end())return;
    for(auto i=iter->second.begin();i!=iter->second.end();i++){
        if(*i==uid1){
            iter->second.erase(i);
            break;
        }
    }
    if(iter->second.empty())listMap.erase(iter);
}

void LockTable::putIntoList(std::unordered_map<long long, std::list<long long>>& listMap, long uid0, long uid1){
    listMap[uid0].push_front(uid1);
}

bool LockTable::isInList(std::unordered_map<long long, std::list<long long>>& listMap, long uid0, long uid1){
    auto iter=listMap.find(uid0);
    if(iter==listMap.end())return false;
    for(long long  uid:iter->second){
        if(uid==uid1)return true;
    }
    return false;
}

static std::shared_ptr<VersionManager> versionManager=nullptr;
static std::mutex mutex2;

std::shared_ptr<VersionManager> VersionManager::instance(){
    // 懒汉模式
    // 使用双重检查保证线程安全
    if(versionManager==nullptr){
        std::unique_lock<std::mutex> lock(mutex2); // 访问临界区之前需要加锁
        if(versionManager==nullptr){
            versionManager=std::shared_ptr<VersionManager>(new VersionManager());
        }
    }
    return versionManager;
}

void VersionManager::init(){
    activeTransaction.insert({0, nullptr});
}

VersionManager::~VersionManager(){

}

char* VersionManager::read(long long xid,long long uid){
    transactionLock.lock();
    auto iter=activeTransaction.find(xid);
    transactionLock.unlock();

    Entry* entry=get(uid);
    if(Visibility::isVisible(iter->second,entry)){
        char* p=entry->getData();
        release(entry->uid);
        return p;
    }
    else return nullptr;
}

long long VersionManager::insert(long long xid,std::vector<char>& data){
    transactionLock.lock();
    auto iter=activeTransaction.find(xid);
    transactionLock.unlock();
    std::vector<char> entry=Entry::makeEntry(data,xid);
    return DataManager::instance()->insert(xid,entry);
}

bool VersionManager::del(long long xid,long long uid){
    transactionLock.lock();
    auto iter=activeTransaction.find(xid);
    transactionLock.unlock();

    Entry* entry=get(uid);
    bool result;
    if(!Visibility::isVisible(iter->second,entry))result= false;
    else{
        std::mutex* lock=LockTable::instance()->add(xid,uid);
        if(lock!= nullptr){
            lock->lock();
            lock->unlock();
        }
        if(entry->getXDEL()==xid)result= false;
        else{
            entry->setXDEL(xid);
            result=true;
        }
    }
    release(entry->uid);
    return result;
}

long long VersionManager::begin(int level){
    transactionLock.lock();
    long long xid=TransactionManager::instance()->begin();
    Transaction* t=Transaction::newTransaction(xid,level,activeTransaction);
    activeTransaction.insert({xid,t});
    transactionLock.unlock();
}

void VersionManager::commit(long long xid){
    transactionLock.lock();
    auto iter=activeTransaction.find(xid);
    transactionLock.unlock();
    transactionLock.lock();
    activeTransaction.erase(iter);
    transactionLock.unlock();
    LockTable::instance()->remove(xid);
    TransactionManager::instance()->commit(xid);

}

void VersionManager::abort(long long xid){
    transactionLock.lock();
    auto iter=activeTransaction.find(xid);
    activeTransaction.erase(iter);
    transactionLock.unlock();
    if(iter->second->autoAborted)return;
    LockTable::instance()->remove(xid);
    TransactionManager::instance()->abort(xid);
}

Entry* VersionManager::get(long long uid){
    // 尝试获取资源
    while(true){
        resourceLock.lock();
        if(getting.find(uid)!=getting.end()){
            // 如果该实体的key在getting中，说明该实体正在被其他线程获取
            resourceLock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(1)); // 让线程休眠1ms后再尝试获取
            continue;
        }
        if(cache.find(uid)!=cache.end()){
            // 没有其他线程正在获取且实体在缓存中
            Entry* entry=cache[uid];
            references[uid]++;
            resourceLock.unlock();
            return entry;
        }
        if(count==maxItemNumber){
            // 如果缓存已满则抛出异常
            resourceLock.unlock();
            throw "cache is full!";
        }
        // 如果该实体没有正被获取，且没有在缓存中，且缓存未满；则将该资源加入到缓存中
        count++;
        getting.insert(std::pair<long long,bool>(uid,true));
        resourceLock.unlock();
        break;
    }

    Entry* entry=getForCache(uid);
    std::unique_lock<std::mutex> lock(resourceLock);
    getting.erase(uid);
    cache.insert(std::pair<long long,Entry*>(uid,entry));
    references.insert(std::pair<long long,int>(uid,1));
    return entry;
}

void VersionManager::release(long long uid){
    std::unique_lock<std::mutex> lock(resourceLock);
    int ref=references[uid]-1; // 该资源当前的引用计数
    if(ref!=0){
        references[uid]=ref;
    }else{
        // 引用计数减为0，应将该资源逐出
        Entry* entry=cache[uid];
        releaseForCache(entry);
        references.erase(uid);
        cache.erase(uid);
        count--;
    }
}

Entry* VersionManager::getForCache(long long uid){
    Entry* entry=Entry::loadEntry(uid);
    return entry;
}

void VersionManager::releaseForCache(Entry* entry){
    PageCache::instance()->release(entry->dataItem->page->getPageNumber());
}