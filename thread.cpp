

#include "table.h"


pthread_t thd[20];
int done[20];

struct search_arg{
    int64_t left,right;
    int attr,thread_id;
};
void* range_search(void *arg){
    //搜索业务函数
    search_arg *para= (search_arg*) arg;
    done[para->thread_id]=1;
    auto table=record_table::get_instance();
    //获取实例
    auto res1=table->search_record(para->left,para->right,para->attr);
    //调用函数
    puts("");

    printf("thread: %dstart print\n",thd[para->thread_id]);
    //打印结果
    for(auto now:res1){
        for(int i=0;i<10;i++){
            printf("%lld ",now.data[i]);
        }
        puts("");
    }

    printf("thread: %dprint complete\n",thd[para->thread_id]);

    done[para->thread_id]=0;
    return nullptr;
}
struct insert_arg{
    record a;
    int thread_id;
};
void* insert_record(void *arg){
    //插入函数
    insert_arg *para= (insert_arg*) arg;
    done[para->thread_id]=1;
    auto table=record_table::get_instance();
    table->insert_record(para->a);

    printf("thread: %dinsert complete\n",thd[para->thread_id]);
    done[para->thread_id]=0;
    return nullptr;
}

struct create_arg{
    int attr,thread_id;
};
void* create_index(void *arg){
    //创建索引函数
    create_arg *para= (create_arg*) arg;
    done[para->thread_id]=1;
    auto table=record_table::get_instance();
    table->create_index(para->attr);

    printf("thread: %dcreate index complete\n",thd[para->thread_id]);
    done[para->thread_id]=0;
    return nullptr;
}



void insert_data(){
    auto table = record_table::get_instance();
    for(int i=0;i<100;i++){
        record a=gen_record();
        table->insert_record(a);
    }
}

int main(){
#ifndef ONLINE_JUDGE
    //freopen("Ranulx.in","r",stdin);
    //freopen("Ranulx.out","w",stdout);
#endif
    memset(done,0,sizeof(done));
    //insert_data();
    while(1){
        int op=-1;
        scanf("%d",&op);
        int now=-1;
        //输入格式
        //0查询：格式 0 left right attr,表示对第attr条属性查询位于[left,right]区间内的记录
        //1插入：格式 1,表示插入一条随机的记录
        //2创建：格式 2 attr,表示对第attr条属性创建索引
        //3退出：格式 3，表示退出程序 
        for(int i=0;;i=(i+1)%20){
            if(done[i]==0){
                now=i;
                break;
            }
            if(i==20){
                sleep(1);
            }
        }
        if(op==0){
            search_arg arg;
            scanf("%lld %lld %d",&arg.left,&arg.right,&arg.attr);
            arg.thread_id=now;
            pthread_create(&thd[now],NULL,range_search,&arg);
            
        }else if(op==1){
            insert_arg arg;
            arg.a=gen_record();
            arg.thread_id=now;
            pthread_create(&thd[now],NULL,insert_record,&arg);
        }else if(op==2){
            create_arg arg;
            scanf("%d",&arg.attr);
            arg.thread_id=now;
            pthread_create(&thd[now],NULL,create_index,&arg);
        }else if(op==3){
            break;
        }
        sleep(1);
    }
    for(int i=0;i<20;i++){
        if(done[i]==1){
            pthread_join(thd[i],NULL);
        }
    }
}