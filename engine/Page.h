#ifndef PAGE
#define PAGE

#include <vector>
#include <list>
#include <unordered_map>
#include <fstream>
#include <atomic>
#include <mutex>
#include <thread>
#include <random>
#include <memory>

class Page {
public:
    Page(long long pageNumber, std::vector<char>& data,int pageSize);
    void setDirty(bool dirty);
    bool isDirty();
    long long getPageNumber();
    char* getData();
private:
    long long pageNumber; // 页号
    std::vector<char> data; // 实际存储的数据
    bool dirty; // 是否为脏页
};

// 页面信息（页号及空闲空间大小）
struct PageInfo {
    int pageNumber; // 页号
    int freeSpace; // 空闲空间大小
    PageInfo(int pageNumber, int freeSpace) :pageNumber(pageNumber),freeSpace(freeSpace){
    }
};

// 页管理类，负责管理页面中的数据
// 特殊页（第一页）管理：用于有效性检查。db启动时给0~63字节处填入随机字节，db关闭时将其拷贝到64~127字节，用于判断上一次数据库是否正常关闭
// 普通页管理：普通页结构[FreeSpaceOffset][Data]，其中FreeSpaceOffset占2字节 表示空闲位置开始偏移
class PageManager {
public:
    // 特殊页管理
    static void initFirstPage(Page* page); // 初始化一个特殊页。在0-63字节随机填入字节
    static void close(Page* firstPage); // 数据库关闭时的行为：将0-63字节中的数据拷贝到64-127字节
    static bool check(Page* firstPage); // 有效性检查
    // 普通页管理
    static void initPage(Page* page); // 初始化一个普通页
    static void setFSO(Page* page,short offset); // 设置一个页的FSO
    static short getFSO(Page* page); // 获取一个页的FSO（空闲空间偏移）
    static short insertData(Page* page,std::vector<char>& data); // 插入数据，并返回新的FSO
    static void updateData(Page* page,std::vector<char>& data,int start); // 更新页面从start到start+data.size()的字节（不包括前2字节）
    static int getFreeSpaceSize(Page* page); // 获取空闲空间的大小

private:
    static const int checkLength=64; // 校验数据的长度
    static const short offsetLength=sizeof(short); // 偏移量长度
};

class PageIndex; // 声明PageIndex类
class PageCache {
public:
    friend class PageIndex;

    static std::shared_ptr<PageCache> instance(); // 获取PageCache的单例对象
    void init(long long memory); // 初始化PageCache,memory是给缓存分配的内存空间的长度

    long long getPageNumbers(); // 获取当前文件中包含的页面个数
    Page* get(long long pageNumber); // 从缓存中获取一个页面，如果不在缓存中则从文件中载入
    void release(long long pageNumber); // 释放一个页面，如果没有其他使用者引用该页面，将其从缓存中移除
    long long newPage(std::vector<char>& data); // 在文件末尾创建一个新页面，并返回其页号
    void truncate(long long newPageNumber); // 扩展文件，使其可以容纳maxPageNumber个页面
    static int getPageSize(){return pageSize;}

    ~PageCache();
    PageCache(const PageCache&) = delete; // 禁用拷贝构造函数
    PageCache& operator=(const PageCache&) = delete; // 禁用赋值运算符
private:
    PageCache() = default; // 禁用外部构造
    void flush(Page* page); // 将一个页面刷到文件中
    Page* getForCache(long long key); // 根据pageNumber（key）从数据库文件中读取页的数据，并包裹成Page返回。当键值为key的资源不在缓存中时，资源的获取方式
    void releaseForCache(Page* page); // 如果是脏页，则需要把页中存储的数据刷入磁盘。当资源被逐出缓存时的写入行为
    std::fstream file; // 数据存储文件

    std::unordered_map<long long,Page*> cache; // 键值到页面的映射
    std::unordered_map<long long,int> references; // 键值到该页面的引用的个数的映射
    std::unordered_map<long long,bool> getting; // 是否有其他进程正在获取该页面
    static const int pageSize=(1<<12); // 页面大小（这里为4KB）
    long long maxPageNumber; // 缓存最大可缓存的页面数
    std::atomic<long long> pageNumbers; // 文件包含的页面总数
    long long count=0; // 缓存中当前包含的页面个数

    std::mutex fileLock; // 文件访问互斥锁
    std::mutex resourceLock; // 资源访问互斥锁
};

// 页面索引类（可以根据所需的空间快速选择一个合适的页面）
class PageIndex{
public:
    static std::shared_ptr<PageIndex> instance(); // 获取PageIndex的单例对象
    void init(); // 初始化PageIndex

    void add(int pageNumber,int freeSpace); // 将一个页的信息加到pageList中
    PageInfo select(int spaceSize); // 从pageList中选一个空闲空闲略大于spaceSize的页返回

    PageIndex(const PageIndex&) = delete; // 禁用拷贝构造函数
    PageIndex& operator=(const PageIndex&) = delete; // 禁用赋值运算符
private:
    PageIndex() = default; // 禁用外部构造
    static const int levelNum=100; // 空闲空间大小的等级数量
    static const int intervalSize=PageCache::pageSize/levelNum; // 相邻等级相差的空间大小
    std::mutex pagesLock; // 访问互斥锁
    // pageList[i]表示有i个大小为intervalSize的空闲空间的页的列表
    std::vector<std::list<PageInfo>> pageList; // 页链表
};

#endif