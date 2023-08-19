#include "Recover.h"

static std::shared_ptr<Logger> logger=nullptr;
static std::mutex mutex;

std::shared_ptr<Logger> Logger::instance(){
    // 懒汉模式
    // 使用双重检查保证线程安全
    if(logger==nullptr){
        std::unique_lock<std::mutex> lock(mutex); // 访问临界区之前需要加锁
        if(logger==nullptr){
            logger=std::shared_ptr<Logger>(new Logger());
        }
    }
    return logger;
}

bool Logger::init() {
    if(!std::ifstream(".log").good()){
        // LOG文件不存在时，需要创建一个新文件
        std::ofstream temp;
        temp.open(".log", std::ios::out|std::ios::binary);
        int checkSum=0;
        temp.write(reinterpret_cast<char*>(&checkSum),sizeof(int));
        temp.close();
    }
    file.open(".log", std::ios::in|std::ios::out|std::ios::binary);
    // 获取文件长度
    file.seekg(0,std::ios::end); //设置文件指针到文件流的尾部
    long long fileSize = file.tellg(); //读取文件指针的位置
    if(fileSize<4)return false; // 文件长度不足，LOG文件无效
    file.seekg(0);
    file.read(reinterpret_cast<char*>(&xChecksum),sizeof(int));
    checkAndRemoveTail();
    return true;
}

void Logger::log(std::vector<char> data){
    int dataSize=data.size();
    char* p=reinterpret_cast<char*>(&dataSize);
    int checkSum= calCheckSum(0,data);
    char* pp=reinterpret_cast<char*>(&checkSum);
    std::vector<char> log(dataLength+checkSumLength+dataSize);
    std::copy(p,p+sizeof(int),log.begin());
    std::copy(pp,pp+sizeof(int),log.begin()+dataLength);
    std::copy(data.begin(),data.end(),log.begin()+dataLength+checkSumLength);

    std::unique_lock<std::mutex> lock(fileLock);
    file.seekp(0,std::ios::end);
    file.write(&(log[0]),dataLength+checkSumLength+dataSize);
    updateXChecksum(log);
}

std::vector<char> Logger::next(){
    std::vector<char> log=nextLog();
    if(log.empty())return log; // 返回空日志
    else return std::vector<char>(log.begin()+dataLength+checkSumLength,log.end()); // 返回承载的数据
}

void Logger::reset(){
    position=checkSumLength;
}

Logger::~Logger() {
    file.close();
}

int Logger::calCheckSum(int checkSum, std::vector<char>& log) {
    for(char c:log){
        checkSum=checkSum*seed+c;
    }
    return checkSum;
}

void Logger::updateXChecksum(std::vector<char>& log){
    xChecksum= calCheckSum(xChecksum,log);
    file.seekp(0);
    file.write(reinterpret_cast<const char*>(&xChecksum),sizeof(int));
    file.flush();
}

void Logger::checkAndRemoveTail(){
    position=checkSumLength;
    int checkSum=0;
    while(true){
        std::vector<char> log=nextLog();
        if(log.empty())break;
        checkSum=calCheckSum(checkSum,log);
    }
    if(checkSum!=xChecksum){
        throw "log file is valid";
    }
    std::unique_lock<std::mutex> lock(fileLock);
    std::filesystem::resize_file(".log",position);
    position=checkSumLength;
}

std::vector<char> Logger::nextLog() {
    std::vector<char> log;
    std::unique_lock<std::mutex> lock(fileLock);
    // 获取文件长度
    file.seekg(0,std::ios::end); //设置文件指针到文件流的尾部
    long long fileSize = file.tellg(); //读取文件指针的位置

    if(position+dataLength+checkSumLength>=fileSize)return std::vector<char>(0);; // 文件长度不足，无法读出一个完整的日志
    int dataSize=0;
    file.seekg(position);
    file.read(reinterpret_cast<char*>(&dataSize),sizeof(int));
    if(position+dataLength+checkSumLength+dataSize>fileSize)return std::vector<char>(0);; // 文件长度不足，无法读出一个完整的日志

    log.resize(dataLength+checkSumLength+dataSize);
    file.seekg(position);
    file.read(&(log[0]),dataLength+checkSumLength+dataSize);
    std::vector<char> data(log.begin()+dataLength+checkSumLength,log.end());
    int checkSum1= calCheckSum(0,data);
    int checkSum2=0;
    char* p=reinterpret_cast<char *>(&checkSum2);
    std::copy(log.begin()+dataLength,log.begin()+dataLength+checkSumLength,p);
    if(checkSum1!=checkSum2)return std::vector<char>(0); // 校验失败
    position += log.size();
    return log;
}

void Recover::recover(){
    Logger::instance()->reset();
    int maxPageNumber=0;
    while (true){
        std::vector<char> log=Logger::instance()->next();
        if(log.empty())break;
        if(log[0]==insertTypeLog){
            InsertLogInfo ili= parseInsertLog(log);
            if(ili.pageNumber>maxPageNumber){
                maxPageNumber=ili.pageNumber;
            }
        }else{
            UpdateLogInfo uli= parseUpdateLog(log);
            if(uli.pageNumber>maxPageNumber){
                maxPageNumber=uli.pageNumber;
            }
        }
    }
    if(maxPageNumber==0)maxPageNumber=1;
    PageCache::instance()->truncate(maxPageNumber);
    redoTransactions();
    undoTransactions();
}

std::vector<char> Recover::updateLog(long long xid, DataItem& di){
    std::vector<char> log(typeLength+xidLength+uidLength+oldRawLength+di.dataItem.size()+di.oldDataItem.size());
    log[0]=updateTypeLog;
    char* p=reinterpret_cast<char*>(&xid);
    std::copy(p,p+xidLength,log.begin()+typeLength);
    long long uid=di.uid;
    char* pp=reinterpret_cast<char*>(&uid);
    std::copy(pp,pp+uidLength,log.begin()+typeLength+xidLength);
    short oldRawSize=di.oldDataItem.size();
    char* ppp=reinterpret_cast<char*>(&oldRawSize);
    std::copy(ppp,ppp+oldRawLength,log.begin()+typeLength+xidLength+uidLength);

    std::copy(di.oldDataItem.begin(),di.oldDataItem.end(),log.begin()+typeLength+xidLength+uidLength+oldRawLength);
    std::copy(di.dataItem.begin(),di.dataItem.end(),log.begin()+typeLength+xidLength+uidLength+oldRawLength+oldRawSize);
    return log;
}

std::vector<char> Recover::insertLog(long long xid, Page* page, std::vector<char>& raw){
    std::vector<char> log(typeLength+xidLength+pageNumberLength+offsetLength+raw.size());
    log[0]=insertTypeLog;
    char* p=reinterpret_cast<char*>(&xid);
    std::copy(p,p+xidLength,log.begin()+typeLength);
    long long pageNumber=page->getPageNumber();
    char* pp=reinterpret_cast<char*>(&pageNumber);
    std::copy(pp,pp+pageNumberLength,log.begin()+typeLength+xidLength);
    short offset=PageManager::getFSO(page);
    char* ppp=reinterpret_cast<char*>(&offset);
    std::copy(ppp,ppp+offsetLength,log.begin()+typeLength+xidLength+pageNumberLength);
    std::copy(raw.begin(),raw.end(),log.begin()+typeLength+xidLength+pageNumberLength+offsetLength);
    return log;
}

void Recover::redoTransactions() {
    Logger::instance()->reset();
    while (true){
        std::vector<char> log=Logger::instance()->next();
        if(log.empty())break;
        if(log[0]==insertTypeLog){
            InsertLogInfo ili= parseInsertLog(log);
            if(!TransactionManager::instance()->isActive(ili.xid)){
                doInsertLog(log,redo);
            }
        }else{
            UpdateLogInfo uli= parseUpdateLog(log);
            if(!TransactionManager::instance()->isActive(uli.xid)){
                doUpdateLog(log,redo);
            }
        }
    }
}

void Recover::undoTransactions() {
    std::unordered_map<long long,std::list<std::vector<char>>> logCache;
    Logger::instance()->reset();
    while (true){
        std::vector<char> log=Logger::instance()->next();
        if(log.empty())break;
        if(log[0]==insertTypeLog){
            InsertLogInfo ili= parseInsertLog(log);
            if(TransactionManager::instance()->isActive(ili.xid)){
                logCache[ili.xid].push_back(log);
            }
        }else{
            UpdateLogInfo uli= parseUpdateLog(log);
            if(TransactionManager::instance()->isActive(uli.xid)){
                logCache[uli.xid].push_back(log);
            }
        }
    }
    for(auto iter=logCache.begin();iter!=logCache.end();iter++){
        for(auto logIter=iter->second.rbegin();logIter!=iter->second.rend();logIter++){
            if((*logIter)[0]==insertTypeLog){
                doInsertLog(*logIter,undo);
            }else{
                doUpdateLog(*logIter,undo);
            }
        }
        TransactionManager::instance()->abort(iter->first);
    }
}

Recover::UpdateLogInfo Recover::parseUpdateLog(std::vector<char>& log){
    UpdateLogInfo uli;
    std::copy(log.begin()+typeLength,log.begin()+typeLength+xidLength,reinterpret_cast<char*>(&uli.xid));
    long long uid=0;
    std::copy(log.begin()+typeLength+xidLength,log.begin()+typeLength+xidLength+uidLength,reinterpret_cast<char*>(&uid));
    uli.offset = (short)(uid&((1ll<<16)-1));
    uid >>= 32;
    uli.pageNumber=(int)(uid & ((1ll<<32)-1));
    short oldRawSize=0;
    std::copy(log.begin()+typeLength+xidLength+uidLength,log.begin()+typeLength+xidLength+uidLength+oldRawLength,reinterpret_cast<char*>(&oldRawSize));
    std::copy(log.begin()+typeLength+xidLength+uidLength+oldRawLength,log.begin()+typeLength+xidLength+uidLength+oldRawLength+oldRawSize,uli.oldData.begin());
    std::copy(log.begin()+typeLength+xidLength+uidLength+oldRawLength+oldRawSize,log.end(),uli.newData.begin());
    return uli;
}

void Recover::doUpdateLog(std::vector<char>& log, int flag){
    UpdateLogInfo uli= parseUpdateLog(log);
    long long pageNumber=uli.pageNumber;
    short offset=uli.offset;
    std::vector<char> data;
    if(flag==redo){
        data.resize(uli.newData.size());
        std::copy(uli.newData.begin(),uli.newData.end(),data.begin());
    }else{
        data.resize(uli.oldData.size());
        std::copy(uli.oldData.begin(),uli.oldData.end(),data.begin());
    }
    Page* page=PageCache::instance()->get(pageNumber);
    PageManager::updateData(page,data,offset);
    PageCache::instance()->release(pageNumber);
}

Recover::InsertLogInfo Recover::parseInsertLog(std::vector<char>& log){
    InsertLogInfo ili;
    std::copy(log.begin()+typeLength,log.begin()+typeLength+xidLength,reinterpret_cast<char*>(&ili.xid));
    std::copy(log.begin()+typeLength+xidLength,log.begin()+typeLength+xidLength+pageNumberLength,reinterpret_cast<char*>(&ili.pageNumber));
    std::copy(log.begin()+typeLength+xidLength+pageNumberLength,log.begin()+typeLength+xidLength+xidLength+pageNumberLength+offsetLength,reinterpret_cast<char*>(&ili.offset));
    std::copy(log.begin()+typeLength+xidLength+pageNumberLength+offsetLength,log.end(),ili.data.begin());
    return ili;
}

void Recover::doInsertLog(std::vector<char>& log, int flag){
    InsertLogInfo ili= parseInsertLog(log);
    Page* page=PageCache::instance()->get(ili.pageNumber);
    if(flag==undo){
        ili.data[0]=1; // 撤销插入，因此将相应的有效位设为无效
    }
    PageManager::updateData(page,ili.data,ili.offset);
    PageCache::instance()->release(ili.pageNumber);
}