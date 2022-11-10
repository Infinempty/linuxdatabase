
#include"constant_val.h"
#include "btree.h"
#include <iostream>
#include <random>
#include <ctime>
#include <pthread.h>

struct record{
    int32_t id;
    int64_t data[LENGTH_OF_RECORD];
    record();
    record(const record &);
};

struct record_table{
public:
    static record_table *get_instance(); 
    //单例模式，获取实例
    int read_record(record &,int);
    //读取一条记录
    std::vector<record> search_record(int64_t,int64_t,int);
    //区间搜索
    void create_index(int);
    //对attr属性创建索引
    int insert_record (record &);
    //插入一条记录
    void print();
    //调试函数
    void* save();
    //保存索引标记
private:
    int pid; 
    //文件标识符
    int num_of_record; 
    //记录数量
    static record_table* m_instance;
    //单例模式的实例
    bool is_index_exists[LENGTH_OF_RECORD]; 
    //记录索引是否存在
    btree index[LENGTH_OF_RECORD];  
    //索引
    pthread_rwlock_t rwlock;        
    //读写锁，用以实现同步
    std::vector<record> direct_search_record(int64_t,int64_t,int);  
    //直接搜索
    std::vector<record> search_record_with_index(int64_t,int64_t,int); 
    //索引搜索

    record_table();
    ~record_table();
};

record gen_record(); 
//生成数据

