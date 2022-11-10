
#include "table.h"

record::record(){
    id=0;
    memset(data,0,sizeof(data));
}
record::record(const record& rhs){
    id=rhs.id;
    for(int i=0;i<LENGTH_OF_RECORD;i++){
        data[i]=rhs.data[i];
    }
}


record_table* record_table::get_instance()
{
    if (nullptr == m_instance )
    {
        m_instance = new record_table();
    }
    return m_instance;
}

record_table::record_table()
{
    //difference between windows and linux in open function
    //pid = open(TABLE_NAME, O_RDWR | O_APPEND | O_CREAT | O_BINARY, S_IRWXU |S_IRWXG|S_IRWXO );
    //创建实例时需要读取文件，在创建时读取的好处是不用重复读取
    pid = open(TABLE_NAME, O_RDWR | O_APPEND | O_CREAT , S_IRWXU |S_IRWXG|S_IRWXO );
    //索引是否存在的标记和数据是分开存储的
    int tmp = open(INDEX_EXISTS_NAME, O_RDWR, S_IRWXU |S_IRWXG|S_IRWXO);
    //int tmp = open(INDEX_EXISTS_NAME, O_RDWR| O_BINARY, S_IRWXU |S_IRWXG|S_IRWXO);
    if (tmp == -1)
    {
        //索引是否存在的文件不存在，则新建
        //tmp = open(INDEX_EXISTS_NAME, O_RDWR | O_CREAT| O_BINARY, S_IRWXU |S_IRWXG|S_IRWXO);
        tmp = open(INDEX_EXISTS_NAME, O_RDWR | O_CREAT, S_IRWXU |S_IRWXG|S_IRWXO);
        for (int i = 0; i < LENGTH_OF_RECORD; i++)
        {
            is_index_exists[i] = 0;
        }
        write(tmp, is_index_exists, LENGTH_OF_RECORD*BYTES_OF_BOOL);
        //记录数量和索引是否存在存储在同一文件
        num_of_record = 0;
        write(tmp, &num_of_record , BYTES_OF_INT);
    }else{
        read(tmp,is_index_exists,LENGTH_OF_RECORD);
        read(tmp, &num_of_record, BYTES_OF_INT);
        for(int i=0;i<LENGTH_OF_RECORD;i++){
            if(is_index_exists[i]){
                index[i].create(i);
            }
        }
    }
    //初始化读写锁
    int ok= pthread_rwlock_init(&rwlock,NULL);
}
record_table::~record_table(){
    save();
    pthread_rwlock_destroy(&rwlock);
}
void* record_table::save(){
    //该函数主要保存索引是否存在
    //int tmp = open(INDEX_EXISTS_NAME, O_RDWR | O_BINARY, S_IRWXU |S_IRWXG|S_IRWXO);
    int tmp = open(INDEX_EXISTS_NAME, O_RDWR , S_IRWXU |S_IRWXG|S_IRWXO);
    lseek(tmp,0,SEEK_SET);
    //先读取索引标记，再读取记录数量
    write(tmp, is_index_exists, LENGTH_OF_RECORD*BYTES_OF_BOOL);
    lseek(tmp,LENGTH_OF_RECORD*BYTES_OF_BOOL,SEEK_SET);
    write(tmp, &num_of_record , BYTES_OF_INT);
    int ok=1;
    return &ok;
}

void record_table::create_index(int idx_id){
    //创建索引
    pthread_rwlock_wrlock(&rwlock);
    //此时上写锁
    if(is_index_exists[idx_id]){
        return;
    }
    record a;
    is_index_exists[idx_id]=1;
    index[idx_id].create(idx_id);
    for(int i=0;i<num_of_record;i++){
        //将表中数据逐条插入
        read_record(a,i);  
        index[idx_id].insert(a.data[idx_id],i);
    }
    save();

    pthread_rwlock_unlock(&rwlock);
}

int record_table::read_record(record &a,int id){
    //按偏移量读取一条记录
    if(id>=num_of_record){
        return -1;
    }
    int x=lseek(this->pid, id*BYTES_OF_RECORD, SEEK_SET);
    
    int cnt=read(this->pid, a.data, BYTES_OF_RECORD);
    a.id=id;
    return cnt==BYTES_OF_RECORD?1:-1;
}
int record_table::insert_record(record &a){

    //插入一条记录到表的末尾

    pthread_rwlock_wrlock(&rwlock);
    //lseek(this->pid,0,SEEK_END);
    int x=write(this->pid,a.data,BYTES_OF_RECORD);
    a.id=this->num_of_record;
    this->num_of_record ++;
    save();
    //如果存在索引则需要更新索引
    for(int i=0;i<LENGTH_OF_RECORD;i++){
        if(is_index_exists[i]){
            index[i].insert(a.data[i],a.id);
        }
    }
    pthread_rwlock_unlock(&rwlock);
    return x==BYTES_OF_RECORD?1:-1;
}



std::vector<record> record_table::direct_search_record(int64_t left,int64_t right,int attr){
    //顺序搜索
    printf("start direct search%d\n",attr);
    record a;
    std::vector<record> res;
    for(int i=0;i<num_of_record;i++){
        //逐条读取记录，将满足条件的放入容器中
        read_record(a,i);
        if(a.data[attr]>=left && a.data[attr]<=right){
            res.emplace_back(a);
        }
    }
    return res;
}



std::vector<record> record_table::search_record_with_index(int64_t left, int64_t right,int attr){

    printf("start search with index %d\n",attr);
    //按索引搜索
    std::vector<record> res;
    auto index_res=index[attr].range_search(left,right);
    //先获取索引保存的偏移量，对应返回值为std::vector<int>
    record tmp;
    for(auto now:index_res){
        //逐条读取，并放入容器中
        read_record(tmp,now);
        res.emplace_back(tmp);
    }
    return res;
}

std::vector<record> record_table::search_record(int64_t left, int64_t right,int attr){
    //面向用户的接口
    //针对第attr条属性，搜索[left,right]区间内的所有记录
    pthread_rwlock_rdlock(&rwlock);
    std::vector<record> res;

    if(is_index_exists[attr]){
        //有索引，用索引搜索
        res=search_record_with_index(left,right,attr);
    }else{
        //如果没有索引，则顺序搜索
        res=direct_search_record(left,right,attr);
    }

    pthread_rwlock_unlock(&rwlock);
    return res;
}

void record_table::print(){
    record a;
    for(int i=0;i<num_of_record;i++){
        read_record(a,i);
        for(int j=0;j<LENGTH_OF_RECORD;j++){
            printf("%lld ",a.data[j]);
        }
        puts("");
    }
}


/*************************************************************
 * **************************************************************
 * ******************************************************
*/
void when_exit(){
    auto i = record_table::get_instance();
    i->save();
}
record_table* record_table::m_instance = NULL;

std::mt19937 rnd(time(NULL));

record gen_record(){
    
    record a;
    for(int i=0;i<LENGTH_OF_RECORD;i++){
        a.data[i]=rnd()%2000;
    }
    return a;
}
// int main()
// {


// #ifndef ONLINE_JUDGE
//     freopen("Ranulx.out","w",stdout);
// #endif
//     atexit(when_exit);
//     auto table = record_table::get_instance();
//     for(int i=0;i<100;i++){
//         record a=gen_record();
//         table->insert_record(a);
//     }
//     table->print();
//     table->create_index(0);
//     auto res=table->search_record(0,99,0);
//     puts("");
//     for(auto now:res){
//         for(int i=0;i<10;i++){
//             printf("%lld ",now.data[i]);
//         }
//         puts("");
//     }
//     auto res1=table->direct_search_record(0,99,0);
//     puts("");
//     for(auto now:res1){
//         for(int i=0;i<LENGTH_OF_RECORD;i++){
//             printf("%lld ",now.data[i]);
//         }
//         puts("");
//     }
// }