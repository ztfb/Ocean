#include "Data.h"

DataItem::DataItem(Page* page,std::vector<char>& dataItem,std::vector<char>& oldDataItem,long long uid)
:page(page),dataItem(dataItem),oldDataItem(oldDataItem),uid(uid)
{}

void DataItem::setValid(bool valid){
    dataItem[0]=(valid?0:1);
}

bool DataItem::isValid(){
    return dataItem[0]==0;
}

char* DataItem::getData() {
    return &(dataItem[validFlagLen+dataSizeLen]);
}

void DataItem::before(){
    writeLock.lock();
    page->setDirty(true);
    // 保存前相数据
    oldDataItem.resize(dataItem.size());
    std::copy(dataItem.begin(),dataItem.end(),oldDataItem.begin());
}

void DataItem::unBefore(){
    // 用前相数据重做
    dataItem.resize(oldDataItem.size());
    std::copy(oldDataItem.begin(),oldDataItem.end(),dataItem.begin());
    writeLock.unlock();
}

void DataItem::after(long long xid){
    // 为XID生成日志
    std::vector<char> log=Recover::updateLog(xid,*this);
    Logger::instance()->log(log);
    writeLock.unlock();
}

DataItem* DataItem::parseDataItem(Page* page,short offset){
    char* p=page->getData()+offset;
    short dataSize=0;
    char* pp=reinterpret_cast<char*>(&dataSize);
    std::copy(p+validFlagLen,p+validFlagLen+dataSizeLen,pp);
    short dataItemSize=validFlagLen+dataSizeLen+dataSize;
    long long uid=(page->getPageNumber())<<32|(long long)(offset);
    std::vector<char> data(p,p+dataItemSize);
    std::vector<char> oldData(dataItemSize);
    return new DataItem(page,data,oldData,uid);
}

std::vector<char> DataItem::construct(std::vector<char>& data){
    std::vector<char> dataItem(validFlagLen+dataSizeLen+data.size());
    short size=data.size();
    char* p=reinterpret_cast<char*>(&size);
    std::copy(p,p+dataSizeLen,dataItem.begin()+validFlagLen);
    std::copy(data.begin(),data.end(),dataItem.begin()+validFlagLen+dataSizeLen);
    return dataItem;
}

static std::shared_ptr<DataManager> dataManager=nullptr;
static std::mutex mutex;

std::shared_ptr<DataManager> DataManager::instance(){
    // 懒汉模式
    // 使用双重检查保证线程安全
    if(dataManager==nullptr){
        std::unique_lock<std::mutex> lock(mutex); // 访问临界区之前需要加锁
        if(dataManager==nullptr){
            dataManager=std::shared_ptr<DataManager>(new DataManager());
        }
    }
    return dataManager;
}

void DataManager::init(long long memory){
    bool isCreate= !std::ifstream(".db").good();
    PageCache::instance()->init(memory);
    Logger::instance()->init();
    if(isCreate){ // 如果各种文件都是新建的
        initFirstPage();
        initPageIndex();
    }else{
        if(!loadFirstPage()){
            // 数据库之前是异常关闭的，需要启动恢复机制
            Recover::recover();
        }
        initPageIndex();
        Page* page=PageCache::instance()->get(1);
        PageManager::initFirstPage(page);
        PageCache::instance()->release(1);
    }
}

DataItem* DataManager::read(long long uid){
    DataItem* di= get(uid);
    if(!di->isValid()){
        release(uid);
        return nullptr;
    }
    return di;
}

long long DataManager::insert(long xid,std::vector<char>& data){
    std::vector<char> dataItem=DataItem::construct(data);
    Page* page;
    for(int i=0; i<10;i ++){
        PageInfo pi=PageIndex::instance()->select(dataItem.size());
        if(pi.pageNumber>0){
            page=PageCache::instance()->get(pi.pageNumber);
            break;
        }else{
            std::vector<char> data(PageCache::getPageSize());
            Page newPage(0,data,PageCache::getPageSize());
            PageManager::initPage(&newPage);
            std::copy(newPage.getData(),newPage.getData()+PageCache::getPageSize(),data.begin());
            int newPageNumber=PageCache::instance()->newPage(data);
            PageIndex::instance()->add(newPageNumber,PageManager::getFreeSpaceSize(&newPage));
        }
    }
    // 记录一条insert日志
    std::vector<char> log=Recover::insertLog(xid,page,dataItem);
    Logger::instance()->log(log);

    short offset=PageManager::insertData(page,dataItem);
    PageIndex::instance()->add(pi.pageNumber,PageManager::getFreeSpaceSize(page));
    PageCache::instance()->release(page->getPageNumber());
    return (page->getPageNumber())<<32|(long long)(offset);
}

void DataManager::initFirstPage(){
    std::vector<char> data(PageCache::getPageSize());
    Page page(0,data,PageCache::getPageSize());
    PageManager::initFirstPage(&page);
    std::copy(page.getData(),page.getData()+PageCache::getPageSize(),data.begin());
    int pageNumber=PageCache::instance()->newPage(data);
    if(pageNumber!=1){
        throw "init fail";
    }
}

bool DataManager::loadFirstPage(){
    Page* page=PageCache::instance()->get(1);
    bool valid=PageManager::check(page);
    PageCache::instance()->release(1);
    return valid;
}

void DataManager::initPageIndex(){
    PageIndex::instance()->init();
    int pageNumbers=PageCache::instance()->getPageNumbers();
    for(int i=2;i<=pageNumbers;i++){
        Page* page=PageCache::instance()->get(i);
        PageIndex::instance()->add(page->getPageNumber(),PageManager::getFreeSpaceSize(page));
        PageCache::instance()->release(i);
    }
}

DataItem* DataManager::get(long long uid){
    // 尝试获取资源
    while(true){
        resourceLock.lock();
        if(getting.find(uid)!=getting.end()){
            // 如果该数据项的key在getting中，说明该数据项正在被其他线程获取
            resourceLock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(1)); // 让线程休眠1ms后再尝试获取
            continue;
        }
        if(cache.find(uid)!=cache.end()){
            // 没有其他线程正在获取且数据项在缓存中
            DataItem* data=cache[uid];
            references[uid]++;
            resourceLock.unlock();
            return data;
        }
        if(count==maxItemNumber){
            // 如果缓存已满则抛出异常
            resourceLock.unlock();
            throw "cache is full!";
        }
        // 如果该数据项没有正被获取，且没有在缓存中，且缓存未满；则将该资源加入到缓存中
        count++;
        getting.insert(std::pair<long long,bool>(uid,true));
        resourceLock.unlock();
        break;
    }

    DataItem* data=getForCache(uid);
    std::unique_lock<std::mutex> lock(resourceLock);
    getting.erase(uid);
    cache.insert(std::pair<long long,DataItem*>(uid,data));
    references.insert(std::pair<long long,int>(uid,1));
    return data;
}

void DataManager::release(long long uid){
    std::unique_lock<std::mutex> lock(resourceLock);
    int ref=references[uid]-1; // 该资源当前的引用计数
    if(ref!=0){
        references[uid]=ref;
    }else{
        // 引用计数减为0，应将该资源逐出
        DataItem* data=cache[uid];
        releaseForCache(data);
        references.erase(uid);
        cache.erase(uid);
        count--;
    }
}

DataManager::~DataManager(){
    Page* page=PageCache::instance()->get(1);
    PageManager::close(page);
    PageCache::instance()->release(1);
}

DataItem* DataManager::getForCache(long long uid){
    short offset=(short)(uid&((1ll<<16)-1)); // 从uid中取出偏移
    uid>>=32;
    long long pageNumber=(long long)(uid&((1ll<<32)-1));
    Page* page=PageCache::instance()->get(pageNumber);
    return DataItem::parseDataItem(page,offset);
}

void DataManager::releaseForCache(DataItem* di){
    PageCache::instance()->release(di->page->getPageNumber());
}