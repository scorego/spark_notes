[toc]

> 提交Spark作业会用到一些脚本，那么这些脚本具体做了什么事情呢？



注：

* 版本：3.1.1
* 时间：2021.03



# 一、 spark-shell

## 1. 文件位置

 `bin/spark-shell`

## 2. 文件功能

 Shell script for starting the Spark Shell REPL.

## 3. 具体逻辑

该脚本核心处理就3步：

1. 确定`${SPARK_HOME}`

   ```shell
   if [ -z "${SPARK_HOME}" ]; then
     source "$(dirname "$0")"/find-spark-home
   fi
   ```

2. 调用`spark-submit`脚本，且指定class为`org.apache.spark.repl.Main`

   ```shell
   function main() {
     if $cygwin; then
       stty -icanon min 1 -echo > /dev/null 2>&1
       export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Djline.terminal=unix"
       "${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" "$@"
       stty icanon echo > /dev/null 2>&1
     else
     	# 看这里
       export SPARK_SUBMIT_OPTS
       "${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" "$@"
     fi
   }
   
   main "$@"
   ```

3. 以`main`方法的返回值退出

   ```shell
   # record the exit status lest it be overwritten:
   # then reenable echo and propagate the code.
   exit_status=$?
   onExit
   ```

可以看到，`spark-shell`实际上就是执行了`spark-submit`脚本，和提交其他普通spark作业是一样的，只不过主类是`org.apache.spark.repl.Main`。调用`spark-submit`的时候传了3个参数。分别是：

- `--class org.apache.spark.repl.Main`
- `--name "Spark shell"`
- `"$@"`

### * 举例 

假如我们调用`spark-shell`脚本的命令是：

```shell
./bin/spark-shell  --master spark://host:7077
```

那么实际上就相当于执行：

```shell
"${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" --master spark://host:7077
```

# 二、 spark-submit

## 1. 文件位置

`bin/spark-submit`

## 2. source code

```shell
if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

# disable randomized hash for string in Python 3.3+
export PYTHONHASHSEED=0

exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
```

`spark-submit`脚本是提交Spark作业的入口脚本，而根据最终执行的命令可以看到，`spark-class` 脚本才是真正的提交程序的，`spark-submit` 脚本只是在其上封装一层，并将 `org.apache.spark.deploy.SparkSubmit` 作为参数传递给`spark-class`来提交作业，由此可见，Spark启动了以`org.apache.spark.deploy.SparkSubmit`为主类的JVM进程。

## 3. 使用参数

```
Usage: spark-submit [options] <app jar | python file | R file> [app arguments]
Usage: spark-submit --kill [submission ID] --master [spark://...]
Usage: spark-submit --status [submission ID] --master [spark://...]
Usage: spark-submit run-example [options] example-class [example args]
```

| Options                        | 含义                                                         | 备注                                                         |
| :----------------------------- | :----------------------------------------------------------- | ------------------------------------------------------------ |
| `--master MASTER_URL`          | spark://host:port, mesos://host:port, yarn,                              k8s://https://host:port, or local | Default: local[*]                                            |
| `--deploy-mode DEPLOY_MODE`    | client  or cluster                                           | Default: client                                              |
| `--class CLASS_NAME`           | application的主类(for  Java/Scala apps)                      | 这个是Spark应用程序真正的主类。                              |
| `--name NAME`                  | application  name                                            |                                                              |
| `--jars JARS`                  | 会放到driver和executor的classpath下的jar包                   | 逗号分隔                                                     |
| `--packages`                   | 会放到driver和executor的classpath下的jar包(maven coordinates) | 逗号分隔；会首先搜索本地maven仓库，然后是maven central和additional remote(通过--repositories指定)；格式：groupId:artifactId:version |
| `--exclude-packages`           | 在--packages中排除的jar包，可以避免依赖冲突                  | 格式：groupId:artifactId，逗号分隔                           |
| `--repositories`               | additional remote  repositories                              | 逗号分隔                                                     |
| `--py-files    PY_FILES`       | 会放到`PYTHONPATH`下的.zip/.egg/.py文件                      | 逗号分隔                                                     |
| `--files  FILES`               | 会放到各个executor工作目录的文件                             | 逗号分隔；executor中，文件路径可以通过SparkFiles.get(fileName)得到 |
| `--archives ARCHIVES`          | arichives会被抽出到各个executor的工作目录                    | 逗号分隔                                                     |
| `--conf`, `-c `     PROP=VALUE | 任意的Spark配置                                              |                                                              |
| `--properties-file FILE`       | 额外配置文件的路径                                           | 如果未指定，会默认搜索conf/spark-defaults.conf               |
| `--driver-memory MEM`          | driver内存大小                                               | e.g. 1000M, 2G；Default: 1024M                               |
| `--driver-java-options`        | Extra Java options to pass to the driver                     |                                                              |
| `--driver-library-path`        | Extra library path entries to pass to the driver             |                                                              |
| `--driver-class-path`          | Extra class path entries to pass to the driver               | 注意：通过--jars添加的jar包会自动添加到classpath中           |
| `--executor-memory MEM`        | executor内存大小                                             | e.g. 1000M, 2G；Default: 1G                                  |
| `--proxy-user   NAME`          | User to impersonate when submitting the application          | This argument does not work with --principal / --keytab      |
| `--help`, `-h`                 | Show this help message and exit                              |                                                              |
| `--verbose`,  `-v`             | Print additional debug output                                |                                                              |
| `--version`                    | Print the version of current Spark                           |                                                              |
| `--driver-cores NUM`           | driver使用的core数                                           | Cluster deploy mode only；Default：1                         |
| `--supervise`                  | If given, restarts the driver on failure                     | Spark standalone or Mesos with cluster deploy mode only      |
| `--kill    SUBMISSION_ID`      | If given, kills the driver specified                         | Spark standalone, Mesos or K8s with cluster deploy mode only |
| `--status   SUBMISSION_ID`     | If given, requests the status of the driver specified        | Spark standalone, Mesos or K8s with cluster deploy mode only |
| `--total-executor-cores NUM`   | Total cores for all executors                                | Spark standalone, Mesos and Kubernetes only                  |
| --executor-cores  NUM          | Number of cores used by each executor                        | Spark standalone, YARN and Kubernetes only；Default: 1 in  YARN and K8S modes, or all available cores on the worker in standalone mode |
| `--num-executors NUM`          | Number of executors to launch；If dynamic allocation is enabled, the initial number of executors will be at least NUM | Spark on YARN and Kubernetes only；Default:2                 |
| `--principal   PRINCIPAL`      | Principal to be used to login to KDC                         | Spark on YARN and Kubernetes only                            |
| `--keytab    KEYTAB`           | The full path to the file that contains the keytab for the principal specified above | Spark on YARN and Kubernetes only                            |
| `--queue QUEUE_NAME`           | The YARN queue to submit to                                  | Spark on YARN only；Default: default                         |

## 4. 使用举例

### ① 提交普通Spark作业

一个使用`spark-submit`提交Spark作业的例子如下：

```shell
./bin/spark-submit \
  --class com.example.WordCount \
  --master spark://127.0.0.1:7077 \
  --deploy-mode cluster \
  --executor-memory 4G \
  --total-executor-cores 20 \
  --supervise \
  ./user-jar-0.1.1.jar \
  arg1 \
  arg2
```

实际上相当于执行：

```shell
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit \
          --class com.example.WordCount \
          --master spark://127.0.0.1:7077 \
          --deploy-mode cluster \
          --executor-memory 4G \
          ./user-jar-0.1.1.jar \
         arg1 \
         arg2
```

### ② 启动spark-shell作业

上一小节说到调用`spark-shell`脚本相当于调用了`spark-submit`脚本：

```shell
./bin/spark-shell  --master spark://host:7077
相当于：
"${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" --master spark://host:7077
```

那么实际上就相当于执行：

```shell
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit \
         --class org.apache.spark.repl.Main \
         --name "Spark shell" \
         --master spark://host:7077
```

# 三、 spark-class

## 1. 文件位置

 `bin/spark-class`

## 2. source code

```shell
1. 检查${SPARK_HOME}是否被设置
if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

2. load spark-env，导入Spark运行的环境变量，获取和验证Spark Scala的版本
. "${SPARK_HOME}"/bin/load-spark-env.sh

3. 获取java的路径，用于后面的用户程序运行
if [ -n "${JAVA_HOME}" ]; then            # 如果JAVA_HOME环境变量已设置，直接获取java脚本路径
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then       #如果 JAVA_HOME环境变量没设置，通过 command -v java 命令获得java脚本路径
    RUNNER="java"
  else                                    # 如果没有找到java命令，那么直接退出，把信息重定向到标准错误输出
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

4. Find Spark jars，获取Spark运行相关jar包路径
if [ -d "${SPARK_HOME}/jars" ]; then       # 如果RELEASE文件存在，表明Spark已结合hadoop编译生成的jar包在lib
  SPARK_JARS_DIR="${SPARK_HOME}/jars"
else
  SPARK_JARS_DIR="${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars"
fi

if [ ! -d "$SPARK_JARS_DIR" ] && [ -z "$SPARK_TESTING$SPARK_SQL_TESTING" ]; then
  echo "Failed to find Spark jars directory ($SPARK_JARS_DIR)." 1>&2
  echo "You need to build Spark with the target \"package\" before running this program." 1>&2
  exit 1
else
  LAUNCH_CLASSPATH="$SPARK_JARS_DIR/*"
fi

5. 若有需要，把启动程序构建目录添加到classpath
# Add the launcher build dir to the classpath if requested. 
if [ -n "$SPARK_PREPEND_CLASSES" ]; then
  LAUNCH_CLASSPATH="${SPARK_HOME}/launcher/target/scala-$SPARK_SCALA_VERSION/classes:$LAUNCH_CLASSPATH"
fi

# For tests
if [[ -n "$SPARK_TESTING" ]]; then
  unset YARN_CONF_DIR
  unset HADOOP_CONF_DIR
fi

6. 执行类文件org.apache.spark.launcher.Main，构造执行命令
build_command() {
  "$RUNNER" -Xmx128m $SPARK_LAUNCHER_OPTS -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.Main "$@"
  printf "%d\0" $?
}

set +o posix
CMD=()
DELIM=$'\n'
CMD_START_FLAG="false"
while IFS= read -d "$DELIM" -r ARG; do
  if [ "$CMD_START_FLAG" == "true" ]; then
    CMD+=("$ARG")
  else
    if [ "$ARG" == $'\0' ]; then
      # After NULL character is consumed, change the delimiter and consume command string.
      DELIM=''
      CMD_START_FLAG="true"
    elif [ "$ARG" != "" ]; then
      echo "$ARG"
    fi
  fi
done < <(build_command "$@")

7. 执行命令(spark-submit脚本传来的是执行org.apache.spark.deploy.SparkSubmit类，此外还有一些其他的)
COUNT=${#CMD[@]}
LAST=$((COUNT - 1))
LAUNCHER_EXIT_CODE=${CMD[$LAST]}
CMD=("${CMD[@]:0:$LAST}")
exec "${CMD[@]}"
```

总的来说，`spark-class`就是导入Spark运行的环境变量，获取java命令路径、相关与Spark运行相关的jar包，获取命令行的参数等，并最终通过 `org.apache.spark.launcher.Main`这个类来启动Spark应用程序。大致步骤是：

1. 检查`SPARK_HOME`变量
2. 执行`load-spark-env.sh`文件
3. 检查java执行路径
4. 查找Spark jars
5. 执行类`org.apache.spark.launcher.Main`来解析参数，并构建命令给`CMD`变量
6. 参数检查：判断解析参数是否正确（代表用户设置的参数是否正确）
7. 执行解析后的命令（`spark-submit`脚本传来的是执行`org.apache.spark.deploy.SparkSubmit`类）

## 3. 使用举例

```shell
1. spark-submit脚本参数
./bin/spark-submit \
  --class com.example.WordCount \
  --master spark://127.0.0.1:7077 \
  --deploy-mode cluster \
  --executor-memory 4G \
  ./user-jar-0.1.1.jar \
  arg1 \
  arg2

2. 相当于调用spark-class
exec "${SPARK_HOME}"/bin/spark-class \ 
          org.apache.spark.deploy.SparkSubmit \
          --class com.example.WordCount \
          --master spark://127.0.0.1:7077 \
          --deploy-mode cluster \
          --executor-memory 4G \
          ./user-jar-0.1.1.jar \
         arg1 \
         arg2
         
3. spark-class脚本中使用org.apache.spark.launcher.Main类来构建命令：
java -Xmx128m -cp ...jars org.apache.spark.launcher.Main \
          org.apache.spark.deploy.SparkSubmit \
          --class com.example.WordCount \
          --master spark://127.0.0.1:7077 \
          --deploy-mode cluster \
          --executor-memory 4G \
          ./user-jar-0.1.1.jar \
         arg1 \
         arg2
```



# 四、`org.apache.spark.launcher.Main`类

`org.apache.spark.launcher.Main`类是Spark launch的命令行接口，主要是根据提交的类型spark-submit和spark-class（master、worker、hostoryserver等等），构建对应的命令解析对象`SparkSubmitCommandBuilder`和`SparkClassCommandBuilder`，再通过`buildCommand()`方法构造执行命令。使用方式是：

```
Usage: Main [class] [class args]

该类有两种模式：
1. "spark-submit"
   如果class是"org.apache.spark.deploy.SparkSubmit"，表示SparkLauncher用来启动一个Spark程序。
2. "spark-class"
   如果class是其他值，则其他内部的Spark类会被执行。
```

在`spark-class`脚本中，会使用该类来解析传入的参数：

```shell
build_command() {
  # 其实就是 java -Xmx128m -cp ...jars org.apache.spark.launcher.Main "$@"
  "$RUNNER" -Xmx128m $SPARK_LAUNCHER_OPTS -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.Main "$@"
  printf "%d\0" $?
}
```

## 1. 代码逻辑

```java
public static void main(String[] argsArray) throws Exception {
    // 参数长度至少为1，至少要把类名传进来
    checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

    // 取出第一个参数，也就是class名
    //    e.g. org.apache.spark.deploy.SparkSubmit
    List<String> args = new ArrayList<>(Arrays.asList(argsArray));
    String className = args.remove(0);

    boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
    Map<String, String> env = new HashMap<>();
    List<String> cmd;


    if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
        // 第一种模式：class是org.apache.spark.deploy.SparkSubmit
        try {
            //  e.g.  args:
            //         	--class com.example.WordCount \
            //			--master spark://127.0.0.1:7077 \
            //			--deploy-mode cluster \
            // 			--executor-memory 4G \
            //			./user-jar-0.1.1.jar \
            // 			arg1 \
            // 			arg2 
            AbstractCommandBuilder builder = new SparkSubmitCommandBuilder(args);
            cmd = buildCommand(builder, env, printLaunchCommand);
        } catch (IllegalArgumentException e) {
            printLaunchCommand = false;
            System.err.println("Error: " + e.getMessage());
            System.err.println();

            MainClassOptionParser parser = new MainClassOptionParser();
            try {
                parser.parse(args);
            } catch (Exception ignored) {
                // Ignore parsing exceptions.
            }

            List<String> help = new ArrayList<>();
            if (parser.className != null) {
                help.add(parser.CLASS);
                help.add(parser.className);
            }
            help.add(parser.USAGE_ERROR);
            AbstractCommandBuilder builder = new SparkSubmitCommandBuilder(help);
            cmd = buildCommand(builder, env, printLaunchCommand);
        }
    } else {
        // 第二种模式：class是其他类
        AbstractCommandBuilder builder = new SparkClassCommandBuilder(className, args);
        cmd = buildCommand(builder, env, printLaunchCommand);
    }

    if (isWindows()) {
        System.out.println(prepareWindowsCommand(cmd, env));
    } else {
        // A sequence of NULL character and newline separates command-strings and others.
        System.out.println('\0');

        // In bash, use NULL as the arg separator since it cannot be used in an argument.
        List<String> bashCmd = prepareBashCommand(cmd, env);
        for (String c : bashCmd) {
            System.out.print(c);
            System.out.print('\0');
        }
    }
}

private static List<String> buildCommand(AbstractCommandBuilder builder, 
                                         Map<String, String> env, 
                                         boolean printLaunchCommand) throws IOException, IllegalArgumentException {
    List<String> cmd = builder.buildCommand(env);
    if (printLaunchCommand) {
        System.err.println("Spark Command: " + join(" ", cmd));
        System.err.println("========================================");
    }
    return cmd;
}
```

由此可见，该类的核心实际上是使用`AbstractCommandBuilderd`的子类来解析传入的参数，当传入的class是`org.apache.spark.deploy.SparkSubmit`时，会使用`SparkSubmitCommandBuilder`来解析，其他情况使用`SparkSubmitCommandBuilder`来解析。

最终解析的命令如下格式：

```shell
java -cp ... -Duser.home=/home/work  org.apache.spark.deploy.SparkSubmit --master spark://127.0.0.1:7077 --deploy-mode cluster --class com.example.WordCount --executor-memory 4G ./user-jar-0.1.1.jar arg1 arg2
```

