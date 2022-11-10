#include <cstdint>
#include <vector>
#include <fcntl.h>
#include <string>
#include <queue>
#include <unistd.h>
#include <memory.h>


struct tree_node{
    int64_t key[BPLUE_LEVEL+1]; //键值，5*8字节
    int32_t son_offset[BPLUE_LEVEL+2]; //儿子节点偏移量，6*4字节
    int32_t index[BPLUE_LEVEL+1];  //数据，5*4字节
    int32_t prev_offset,nxt_offset,key_num,id; //前驱，后继，键数量，id，4*4字节
    bool is_leaf; //叶子标记，1字节
};



// ./
struct btree{
private:
    int node_cnt,pid;
    std::string i_name;
    //文件名称
    int read_node(int id,tree_node &a); 
    //读取一个节点
    int write_node(tree_node &x);
    //存储一个节点
    tree_node lower_bound(int64_t val);
    //查询第一个存储了大于等于val的键值的节点
    void split(tree_node&,tree_node&);
    //分裂函数，第一个参数为parent,第二个参数为left_chile,分裂对象是left_child
    int insert_dfs(int64_t val,int idx,tree_node &parent,int id);
    //递归的插入数据
    int insert_tonode(int64_t val,int idx,tree_node &now,int new_offset);
    //将数据直接插入节点中而不考虑分裂
public:
    void create(int id);
    //建树，有文件读文件，没文件创建文件
    std::vector<int> range_search(int64_t left,int64_t right);
    //区间搜索，返回record偏移量
    int insert(int64_t val,int idx);
    //插入函数，第一个参数为键值，第二个参数为record偏移量
    void print();
    //调试函数
};

