#include "constant_val.h"
#include "btree.h"




int btree::read_node(int id,tree_node &a){

    //按节点id读取树上节点，每个节点的长度为BYTES_OF_TREENODE
    //读取顺序：键、儿子节点偏移量、数据（记录的index）、叶子节点的前驱、叶子节点的后继
    // 节点内存储键值数量、节点ID、叶子节点标记
    lseek(pid,id*BYTES_OF_TREENODE,SEEK_SET);
    int cnt=0;
    cnt+=read(pid,a.key,(BPLUE_LEVEL+1)*BYTES_OF_INT64);
    cnt+=read(pid,a.son_offset,(BPLUE_LEVEL+2)*BYTES_OF_INT);
    cnt+=read(pid,a.index,(BPLUE_LEVEL+1)*BYTES_OF_INT);
    //cnt+=read(pid,&a.parent_offset,BYTES_OF_INT);
    cnt+=read(pid,&a.prev_offset,BYTES_OF_INT);
    cnt+=read(pid,&a.nxt_offset,BYTES_OF_INT);
    cnt+=read(pid,&a.key_num,BYTES_OF_INT);
    cnt+=read(pid,&a.id,BYTES_OF_INT);
    cnt+=read(pid,&a.is_leaf,BYTES_OF_BOOL);
    return cnt==BYTES_OF_TREENODE?1:-1;
}

int btree::write_node(tree_node &a){

    //按节点id写节点
    //写顺序：键、儿子节点偏移量、数据（记录的index）、叶子节点的前驱、叶子节点的后继
    // 节点内存储键值数量、节点ID、叶子节点标记
    lseek(pid,a.id*BYTES_OF_TREENODE,SEEK_SET);
    int cnt=0;
    cnt+=write(pid,a.key,(BPLUE_LEVEL+1)*BYTES_OF_INT64);
    cnt+=write(pid,a.son_offset,(BPLUE_LEVEL+2)*BYTES_OF_INT);
    cnt+=write(pid,a.index,(BPLUE_LEVEL+1)*BYTES_OF_INT);
    //cnt+=write(pid,&a.parent_offset,BYTES_OF_INT);
    cnt+=write(pid,&a.prev_offset,BYTES_OF_INT);
    cnt+=write(pid,&a.nxt_offset,BYTES_OF_INT);
    cnt+=write(pid,&a.key_num,BYTES_OF_INT);
    cnt+=write(pid,&a.id,BYTES_OF_INT);
    cnt+=write(pid,&a.is_leaf,BYTES_OF_BOOL);
    return cnt==BYTES_OF_TREENODE?1:-1;
}

tree_node btree::lower_bound(int64_t val){
    //找到保存第一个大于等于val的key的叶子节点
    tree_node now;
    int id=0;
    read_node(id,now);
    while(!now.is_leaf){
        for(int i=0;i<now.key_num;i++){
            if(val< now.key[i]){
                //如果小于当前键，意味着一定要往左儿子走
                id=now.son_offset[i];
                break;
            }
        }
        //按id读取下一个节点
        read_node(id,now);
    }
    return now;
}

std::vector<int> btree::range_search(int64_t left,int64_t right){
    //区间查询，返回一个int的容器表示对应记录在表中的偏移量
    std::vector<int> res;
    tree_node now=lower_bound(left);
    int id=now.id;
    while(1){
        bool ok=0;
        for(int i=0;i<now.key_num;i++){
            if(now.key[i]>=left&&now.key[i]<=right){
                //满足要求则加入
                res.emplace_back(now.index[i]);
            }else if(now.key[i]>right){
                ok=1;
                break;
                //超过查询范围 退出
            }
        }
        if(ok)break;
        if(now.nxt_offset!=-1){
            //判断后继叶子节点是否存在
            id=now.nxt_offset;
            read_node(id,now);
        }else{
            break;
        }
    }
    return res;
}

void btree::create(int id){
    //创建树，如果树已经存在则读取，否则创建
    i_name=INDEX_NAME+std::to_string(id);
    //pid = open(i_name.data(), O_RDWR| O_BINARY, S_IRWXU |S_IRWXG|S_IRWXO);
    pid = open(i_name.data(), O_RDWR, S_IRWXU |S_IRWXG|S_IRWXO);
    if (pid == -1){
        node_cnt=1;
        //pid = open(i_name.data(), O_RDWR | O_CREAT| O_BINARY, S_IRWXU |S_IRWXG|S_IRWXO);
        pid = open(i_name.data(), O_RDWR | O_CREAT, S_IRWXU |S_IRWXG|S_IRWXO);
        tree_node tmp;
        tmp.key_num=0;
        tmp.is_leaf=1;
        tmp.id=0;
        tmp.prev_offset=tmp.nxt_offset=-1;
        memset(tmp.son_offset,-1,sizeof(tmp.son_offset));
        //tmp.parent_offset=-1;
        write_node(tmp);
    }else{
        //计算节点数量
        node_cnt=lseek(pid,0,SEEK_END)/BYTES_OF_TREENODE;
    }
}
int btree::insert_tonode(int64_t val,int idx,tree_node &now,int new_offset){
    //将一条数据插入一个节点，但不考虑是否分裂
    int insert_pos=0;
    for(int i=now.key_num-1;~i;i--){
        if(now.key[i]>=val){
            //后移
            now.key[i+1]=now.key[i];
            now.son_offset[i+2]=now.son_offset[i+1];
            now.index[i+1]=now.index[i];
        }else{
            //对应位置
            insert_pos=i+1;
            break;
        }
    }
    //需要插入的东西有：key，右儿子偏移，以及数据
    now.key[insert_pos]=val;
    now.son_offset[insert_pos+1]=new_offset;
    now.index[insert_pos]=idx;
    now.key_num++;
    return 1;
}

void btree::split(tree_node &parent, tree_node &now){
    //节点分裂，事实上是分裂的now节点，now为left_son
    //并且新建right_son ，中键的key通过insert_tonode函数加入parent
    //注意：根节点不能调用此函数
    tree_node right;
    memset(right.son_offset,-1,sizeof(right.son_offset));
    int prenum=BPLUE_LEVEL/2+1;
    //中间节点
    right.key_num=BPLUE_LEVEL/2;
    right.id=this->node_cnt++;
    right.is_leaf=now.is_leaf;
    now.key_num=BPLUE_LEVEL/2;
    if(now.is_leaf){
        //如果是叶子，中间节点需要保存到左儿子，否则不保存
        now.key_num++;
    }
    for(int i=0;i<right.key_num;i++){
        //复制信息到右儿子
        right.key[i]=now.key[i+prenum];
        right.son_offset[i]=now.son_offset[i+prenum];
        right.index[i]=now.index[i+prenum];
    }
    right.son_offset[right.key_num]=now.son_offset[right.key_num+prenum];

    if(now.is_leaf){
        //如果是叶子，则需要更新前驱和后继
        right.nxt_offset=now.nxt_offset;
        right.prev_offset=now.id;
        now.nxt_offset=right.id;
    }
    write_node(right);
    //将中间的键值加入parent节点
    insert_tonode(now.key[prenum],now.index[prenum],parent,right.id);
}

int btree::insert_dfs(int64_t val,int idx,tree_node &parent,int id){
    //插入一个数据后，树的调整是自下而上的，我们需要先将数据插入叶子节点
    //再逐步调整父亲节点，因此采用dfs的方式调整
    tree_node now;
    read_node(id,now);
    if(now.is_leaf){
        insert_tonode(val,idx,now,-1);
    }else{
        int nxt=now.key_num;
        //找到插入的位置
        for(int i=0;i<now.key_num;i++){
            if(val<now.key[i]){
                nxt=i;
                break;
            }
        }
        //递归的插入
        insert_dfs(val,idx,now,now.son_offset[nxt]);
    }
    if(now.key_num>BPLUE_LEVEL){
        //插入后如果需要分裂，则进行分裂
        //父亲节点的分裂会在其对应的节点进行
        split(parent,now);
    }
    write_node(now);
    return 1;
}

int btree::insert(int64_t val,int idx){
    tree_node root;
    read_node(0,root);
    if(root.is_leaf){
        insert_tonode(val,idx,root,-1);
    }else{
        int insert_pos=root.key_num;
        for(int i=0;i<root.key_num;i++){
            if(val<root.key[i]){
                insert_pos=i;
                break;
            }
        }
        insert_dfs(val,idx,root,root.son_offset[insert_pos]);
    }
    //需要保证root节点的偏移量为0
    //因此root节点的分裂需要特殊处理
    if(root.key_num>BPLUE_LEVEL){
        //root的分裂操作
        //...
        tree_node left,right;
        //新建leftson和rightson，分裂逻辑和先前的split函数一样
        memset(right.son_offset,-1,sizeof(right.son_offset));
        memset(left.son_offset,-1,sizeof(left.son_offset));
        //初始化left,right
        int prenum=BPLUE_LEVEL/2+1;
        left.key_num=prenum-1;
        right.key_num=prenum-1;
        left.id=this->node_cnt++;
        right.id=this->node_cnt++;
        left.is_leaf=right.is_leaf=root.is_leaf;
        if(root.is_leaf){
            left.key_num++;
        }
        for(int i=0;i<left.key_num;i++){
            //左儿子复制信息
            left.key[i]=root.key[i];
            left.son_offset[i]=root.son_offset[i];
            left.index[i]=root.index[i];
        }
        left.son_offset[left.key_num]=root.son_offset[left.key_num];
        for(int i=0;i<right.key_num;i++){
            //右儿子复制信息
            right.key[i]=root.key[i+prenum];
            right.son_offset[i]=root.son_offset[i+prenum];
            right.index[i]=root.index[i+prenum];
        }
        right.son_offset[right.key_num]=root.son_offset[right.key_num+prenum];
        if(root.is_leaf){
            //更新前驱和后继，如果是叶子节点的话
            left.prev_offset=root.prev_offset;
            left.nxt_offset=right.id;
            right.prev_offset=left.id;
            right.nxt_offset=root.nxt_offset;
        }

        root.key_num=1;
        root.key[0]=root.key[prenum-1];
        root.son_offset[0]=left.id;
        root.son_offset[1]=right.id;
        root.is_leaf=0;
        write_node(left);
        write_node(right);
    }
    write_node(root);
    return 1;
}


void btree::print(){
    tree_node root;
    std::queue<int> q;
    q.push(0);
    while(!q.empty()){
        int now=q.front();q.pop();
        read_node(now,root);
        printf("%d:\n",now);
        for(int i=0;i<=root.key_num;i++){
            printf("(%d,%d,%d)",root.key[i],root.son_offset[i],root.index[i]);
            if(!root.is_leaf && root.son_offset[i]!=-1){
                q.push(root.son_offset[i]);
            }
        }
        puts("");
    }
}

// int main(){
//     btree a;
//     a.create(1);
//     tree_node tmp;
//     for(int i=1;i<=15;i++){
//         a.insert(i,15-i);
//         a.read_node(0,tmp);
//         a.read_node(1,tmp);
//         a.read_node(2,tmp);
//     }
// #ifndef ONLINE_JUDGE
//     freopen("Ranulx.out","w",stdout);
// #endif
//     a.print();
//     auto i=a.range_search(2,10);
//     for(auto to:i){
//         printf("%d ",to);
//     }
//     puts("");
// }

