#include "Page.h"

Page::Page(long long pageNumber, std::vector<char>& data,int pageSize):pageNumber(pageNumber){
    this->data.resize(pageSize);
    std::copy(data.begin(),data.end(),this->data.begin());
    this->dirty=false;
}

void Page::setDirty(bool dirty) {
    this->dirty=dirty;
}

bool Page::isDirty() {
    return this->dirty;
}

long long Page::getPageNumber() {
    return this->pageNumber;
}

char* Page::getData() {
    return &(this->data[0]);
}

void PageManager::initFirstPage(Page* page) {
    page->setDirty(true);
    srand(time(0));
    std::vector<char> randomBytes(checkLength);
    for(int i=0;i<checkLength;i++){
        randomBytes[i]=rand()%128;
    }
    std::copy(randomBytes.begin(),randomBytes.end(),page->getData());
}

void PageManager::close(Page* firstPage) {
    firstPage->setDirty(true);
    std::copy(firstPage->getData(),firstPage->getData()+checkLength,firstPage->getData()+checkLength);
}

bool PageManager::check(Page* firstPage) {
    for(int i=0;i<checkLength;i++){
        char* p1=firstPage->getData()+i;
        char* p2=firstPage->getData()+checkLength+i;
        if(*p1!=*p2)return false;
    }
    return true;
}

void PageManager::initPage(Page *page) {
    // FSO的初值设置为offsetLength
    setFSO(page,offsetLength);
}

void PageManager::setFSO(Page* page,short offset){
    page->setDirty(true);
    char* p=reinterpret_cast<char*>(&offset);
    std::copy(p,p+offsetLength,page->getData());
}

short PageManager::getFSO(Page* page){
    short offset=0;
    char* p=reinterpret_cast<char*>(&offset);
    std::copy(page->getData(),page->getData()+offsetLength,p);
    return offset;
}

short PageManager::insertData(Page* page,std::vector<char>& data){
    page->setDirty(true);
    short offset= getFSO(page);
    std::copy(data.begin(),data.end(),page->getData()+offset);
    short newOffset=offset+data.size();
    setFSO(page,newOffset);
    return newOffset;
}

void PageManager::updateData(Page* page,std::vector<char>& data,int start){
    page->setDirty(true);
    std::copy(data.begin(),data.end(),page->getData()+start);
    short oldOffset= getFSO(page);
    // 如果更新后数据的总长度大于旧数据的总长度，则需要更新偏移
    setFSO(page,std::max((int)oldOffset,(int)(offsetLength+start+data.size())));
}

int PageManager::getFreeSpaceSize(Page* page){
    return PageCache::getPageSize()-(int)(getFSO(page));
}

static std::shared_ptr<PageCache> pageCache=nullptr;
static std::mutex mutex;

std::shared_ptr<PageCache> PageCache::instance(){
    // 懒汉模式
    // 使用双重检查保证线程安全
    if(pageCache==nullptr){
        std::unique_lock<std::mutex> lock(mutex); // 访问临界区之前需要加锁
        if(pageCache==nullptr){
            pageCache=std::shared_ptr<PageCache>(new PageCache());
        }
    }
    return pageCache;
}

void PageCache::init(long long memory) {
    this->maxPageNumber=memory/pageSize;
    if(!std::ifstream(".db").good()){
        // DB文件不存在时，需要创建一个新文件
        std::ofstream temp;
        temp.open(".db", std::ios::out|std::ios::binary);
        temp.close();
    }
    file.open(".db", std::ios::in|std::ios::out|std::ios::binary);
    // 获取文件大小
    file.seekg(0,std::ios::end); //设置文件指针到文件流的尾部
    long long size = file.tellg(); //读取文件指针的位置
    pageNumbers=size/pageSize;
}

long long PageCache::getPageNumbers(){
    return this->pageNumbers;
}

Page* PageCache::get(long long pageNumber) {
    // 尝试获取资源
    while(true){
        resourceLock.lock();
        if(getting.find(pageNumber)!=getting.end()){
            // 如果该页面的key在getting中，说明该页面正在被其他线程获取
            resourceLock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(1)); // 让线程休眠1ms后再尝试获取
            continue;
        }
        if(cache.find(pageNumber)!=cache.end()){
            // 没有其他线程正在获取且页面在缓存中
            Page* data=cache[pageNumber];
            references[pageNumber]++;
            resourceLock.unlock();
            return data;
        }
        if(count==maxPageNumber){
            // 如果缓存已满则抛出异常
            resourceLock.unlock();
            throw "cache is full!";
        }
        // 如果该页面没有正被获取，且没有在缓存中，且缓存未满；则将该资源加入到缓存中
        count++;
        getting.insert(std::pair<long long,bool>(pageNumber,true));
        resourceLock.unlock();
        break;
    }

    Page* data=getForCache(pageNumber);
    std::unique_lock<std::mutex> lock(resourceLock);
    getting.erase(pageNumber);
    cache.insert(std::pair<long long,Page*>(pageNumber,data));
    references.insert(std::pair<long long,int>(pageNumber,1));
    return data;
}

void PageCache::release(long long pageNumber) {
    std::unique_lock<std::mutex> lock(resourceLock);
    int ref=references[pageNumber]-1; // 该资源当前的引用计数
    if(ref!=0){
        references[pageNumber]=ref;
    }else{
        // 引用计数减为0，应将该资源逐出
        Page* data=cache[pageNumber];
        releaseForCache(data);
        references.erase(pageNumber);
        cache.erase(pageNumber);
        count--;
    }
}

Page* PageCache::getForCache(long long key){
    // 将key作为pageNumber使用
    long long offset = (key-1)*pageSize; // 计算页面偏移
    std::unique_lock<std::mutex> lock(fileLock);
    file.seekg(offset);
    std::vector<char> data(pageSize);
    file.read(&(data[0]),pageSize);
    return new Page(key,data,pageSize);
}

void PageCache::releaseForCache(Page* page){
    if(page->isDirty()){ // 如果是脏页需要刷回磁盘
        flush(page);
        delete page;
    }
}

long long PageCache::newPage(std::vector<char>& data) {
    pageNumbers++;
    long long pageNumber = pageNumbers;
    Page page(pageNumber,data,pageSize);
    flush(&page);
    return pageNumber;
}

void PageCache::truncate(long long newPageNumber) {
    long long size=newPageNumber*pageSize;
    std::unique_lock<std::mutex> lock(fileLock);
    // 获取文件原始大小
    file.seekg(0,std::ios::end); //设置文件指针到文件流的尾部
    long long oldSize = file.tellg(); //读取文件指针的位置
    char* temp=new char[size-oldSize];
    file.seekp(0,std::ios::end); //设置文件指针到文件流的尾部
    file.write(temp,size-oldSize);
    delete[] temp;
    pageNumbers.store(newPageNumber);
}

void PageCache::flush(Page* page) {
    long long offset = (page->getPageNumber()-1)*pageSize; // 页面偏移
    std::unique_lock<std::mutex> lock(fileLock);
    file.seekp(offset);
    file.write(page->getData(),pageSize);
    file.flush();
}

PageCache::~PageCache() {
    // 关闭缓存，写回所有资源
    std::unique_lock<std::mutex> lock(resourceLock);
    for(auto iter=cache.begin();iter!=cache.end();iter++){
        releaseForCache(iter->second);
    }
    file.close();
}

static std::shared_ptr<PageIndex> pageIndex=nullptr;
static std::mutex mutex2;

std::shared_ptr<PageIndex> PageIndex::instance(){
    // 懒汉模式
    // 使用双重检查保证线程安全
    if(pageIndex==nullptr){
        std::unique_lock<std::mutex> lock(mutex2); // 访问临界区之前需要加锁
        if(pageIndex==nullptr){
            pageIndex=std::shared_ptr<PageIndex>(new PageIndex());
        }
    }
    return pageIndex;
}

void PageIndex::init(){
    pageList.resize(levelNum+1);
}

void PageIndex::add(int pageNumber,int freeSpace){
    std::unique_lock<std::mutex> lock(pagesLock);
    int number=freeSpace/intervalSize;
    pageList[number].emplace_back(pageNumber,freeSpace);
}

PageInfo PageIndex::select(int spaceSize){
    std::unique_lock<std::mutex> lock(pagesLock);
    int number=spaceSize/intervalSize;
    if(number<levelNum)number++;
    while(number<levelNum){
        if(pageList[number].empty()){
            number++;
            continue;
        }
        PageInfo pi=pageList[number].back();
        pageList[number].pop_back();
        return pi;
    }
    // 所需的空闲空间超过了levelNum*intervalSize
    for(auto iter=pageList[levelNum].begin();iter!=pageList[levelNum].end();iter++){
        if(spaceSize<=iter->freeSpace){
            PageInfo pi(iter->pageNumber,iter->freeSpace);
            pageList[levelNum].erase(iter);
            return pi;
        }
    }
    return {-1,0};
}