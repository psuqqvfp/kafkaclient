Kafka Client </br>
使用的新consumer api</br>
多线程安全的consumer</br>
方便以后代码更新使用</br>
必要设置： </br>
1、consumer的线程数量与kafka的topic分区的数量最好保持一致</br>
2、10条数据，1-10，二个分区，二个线程，则分区1获取1，3，5，7，9 分区2获取2，4，6，8，0</br>
3、详见代码</br>
