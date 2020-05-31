$ cd {spark_path}

$ ./bin/spark-submit --class ljl.TestAll --master local --deploy-mode client --executor-memory 4000m \ 
/home/bdms/homework/Spark-B-Tree-master/target/Bigdata-1.0-SNAPSHOT.jar

生成源数据的接口还没改好，先编译两次，第一次只执行generateData()，生成好的数据手动上传到HDFS(之后会追加自动上传)；
第二次把generateData()注释掉，用{spark_path}/bin/spark-submit开启Spark。

目前1000MB的数据集跑出来会报错，其他都可。

画图的python脚本正在准备中。