二次排序原理：
1）自定义键类型，把值放入键中；

2）利用键的排序特性，可以顺便把值也排序了；

3）这时会有两个问题：

a. 数据传输到不同的Reducer会有异常；

b. 数据在Reducer中的分组不同；

针对这两个问题，需要使用自定义Partitioner、使用自定义GroupComparator来定义相应的逻辑；

http://zengzhaozheng.blog.51cto.com/8219051/1379271


原理:
https://www.zybuluo.com/duguyiren3476/note/71375