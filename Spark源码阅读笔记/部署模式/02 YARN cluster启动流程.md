# 一、 提交作业

可以通过`spark-submit`脚本向YARN集群提交作业，假设提交命令如下：

```shell
./bin/spark-submit \
    --class com.example.WordCount \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory 4G \
    ./user-jar-0.1.1.jar \
    arg1 \
    arg2
```

最终执行的java命令是：

```shell
java -cp ... -Duser.home=/home/work  org.apache.spark.deploy.SparkSubmit \
	--master yarn \
	--deploy-mode cluster \
	--class com.example.WordCount \
	--executor-memory 4G \
	./user-jar-0.1.1.jar arg1 arg2
```

