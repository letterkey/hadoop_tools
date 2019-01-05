mapreduce 优化:
1.对象重用:在map或reduce函数外声明变量
2.截取字符串:使用apache的stringutils.split方法比string.split方法效率更高
3.减少map的数据输出:过滤和投影, 过滤:在map中使用过滤条件;投影:取出记录的一部分最为结果输出
