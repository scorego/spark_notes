[toc]

# 一、start-master

## 1. 文件位置

`sbin/start-master.sh`

## 2. 文件功能

Starts the master on the machine this script is executed on.

## 3. 具体逻辑

```shell
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# NOTE: This exact class name is matched downstream by SparkSubmit.
# Any changes need to be reflected there.
CLASS="org.apache.spark.deploy.master.Master"

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-master.sh [options]"
  pattern="Usage:"
  pattern+="\|Using Spark's default log4j profile:"
  pattern+="\|Started daemon with process name"
  pattern+="\|Registered signal handler for"

  "${SPARK_HOME}"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
  exit 1
fi

ORIGINAL_ARGS="$@"

. "${SPARK_HOME}/sbin/spark-config.sh"

. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ "$SPARK_MASTER_PORT" = "" ]; then
  SPARK_MASTER_PORT=7077
fi

if [ "$SPARK_MASTER_HOST" = "" ]; then
  case `uname` in
      (SunOS)
	  SPARK_MASTER_HOST="`/usr/sbin/check-hostname | awk '{print $NF}'`"
	  ;;
      (*)
	  SPARK_MASTER_HOST="`hostname -f`"
	  ;;
  esac
fi

if [ "$SPARK_MASTER_WEBUI_PORT" = "" ]; then
  SPARK_MASTER_WEBUI_PORT=8080
fi

# 这里是实际执行的逻辑，就是调用spark-daemon.sh脚本
"${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS 1 \
  --host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT \
  $ORIGINAL_ARGS
```

```shell
sh -x /usr/local/spark/sbin/spark-daemon.sh start org.apache.spark.deploy.master.Master 1 --host s101 --port 7077 --webui-port 8080
```

# 二、 start-history-server

## 1. 文件位置

`sbin/start-history-server.sh`

## 2. 文件功能

Starts the history server on the machine this script is executed on.

Usage: start-history-server.sh
Use the SPARK_HISTORY_OPTS environment variable to set history server configuration.

## 3. 具体逻辑

```shell
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

CLASS="org.apache.spark.deploy.history.HistoryServer"

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-history-server.sh [options]"
  pattern="Usage:"
  pattern+="\|Using Spark's default log4j profile:"
  pattern+="\|Started daemon with process name"
  pattern+="\|Registered signal handler for"

  "${SPARK_HOME}"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
  exit 1
fi

. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"

exec "${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS 1 "$@"
```



# 三、spark-daemon

## 1. 文件位置

`sbin/spark-daemon.sh`

## 2. 文件功能

Runs a Spark command as a daemon.

```
# Environment Variables
#
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_LOG_DIR   Where log files are stored. ${SPARK_HOME}/logs by default.
#   SPARK_LOG_MAX_FILES Max log files of Spark daemons can rotate to. Default is 5.
#   SPARK_MASTER    host:path where spark code should be rsync'd from
#   SPARK_PID_DIR   The pid files are stored. /tmp by default.
#   SPARK_IDENT_STRING   A string representing this instance of spark. $USER by default
#   SPARK_NICENESS The scheduling priority for daemons. Defaults to 0.
#   SPARK_NO_DAEMONIZE   If set, will run the proposed command in the foreground. It will not output a PID file.
```



## 3. 具体逻辑

```shell

usage="Usage: spark-daemon.sh [--config <conf-dir>] (start|stop|submit|status) <spark-command> <spark-instance-number> <args...>"

# 如果没有提供参数，则打印usage并退出
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# 执行spark-config.sh，用途是设置SPARK_HOME、SPARK_CONF_DIR以及python的一些环境变量
. "${SPARK_HOME}/sbin/spark-config.sh"

# get arguments

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.

if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export SPARK_CONF_DIR="$conf_dir"
  fi
  shift
fi

option=$1
shift
command=$1
shift
instance=$1
shift

spark_rotate_log ()
{
    log=$1;

    if [[ -z ${SPARK_LOG_MAX_FILES} ]]; then
      num=5
    elif [[ ${SPARK_LOG_MAX_FILES} -gt 0 ]]; then
      num=${SPARK_LOG_MAX_FILES}
    else
      echo "Error: SPARK_LOG_MAX_FILES must be a positive number, but got ${SPARK_LOG_MAX_FILES}"
      exit -1
    fi

    if [ -f "$log" ]; then # rotate logs
        while [ $num -gt 1 ]; do
            prev=`expr $num - 1`
            [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
            num=$prev
        done
        mv "$log" "$log.$num";
    fi
}

. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ "$SPARK_IDENT_STRING" = "" ]; then
  export SPARK_IDENT_STRING="$USER"
fi

export SPARK_PRINT_LAUNCH_COMMAND="1"

# get log directory
if [ "$SPARK_LOG_DIR" = "" ]; then
  export SPARK_LOG_DIR="${SPARK_HOME}/logs"
fi
mkdir -p "$SPARK_LOG_DIR"
touch "$SPARK_LOG_DIR"/.spark_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$SPARK_LOG_DIR"/.spark_test
else
  chown "$SPARK_IDENT_STRING" "$SPARK_LOG_DIR"
fi

if [ "$SPARK_PID_DIR" = "" ]; then
  SPARK_PID_DIR=/tmp
fi

# some variables
log="$SPARK_LOG_DIR/spark-$SPARK_IDENT_STRING-$command-$instance-$HOSTNAME.out"
pid="$SPARK_PID_DIR/spark-$SPARK_IDENT_STRING-$command-$instance.pid"

# Set default scheduling priority
if [ "$SPARK_NICENESS" = "" ]; then
    export SPARK_NICENESS=0
fi

execute_command() {
  if [ -z ${SPARK_NO_DAEMONIZE+set} ]; then
  		# 后台执行脚本命令，此处是获取传入该方法的所有参数，然后运行
      nohup -- "$@" >> $log 2>&1 < /dev/null &
      newpid="$!"

      echo "$newpid" > "$pid"

      # 等待上述java进程运行，等待5s
      for i in {1..10}
      do
        if [[ $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
           break
        fi
        sleep 0.5
      done

      sleep 2
      # Check if the process has died; in that case we'll tail the log so the user can see
      if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
        echo "failed to launch: $@"
        tail -10 "$log" | sed 's/^/  /'
        echo "full log in $log"
      fi
  else
      "$@"
  fi
}

# 该方法主要判断是否能继续进行，并做一些预处理
run_command() {
  mode="$1"
  shift

  mkdir -p "$SPARK_PID_DIR"

	# 判断进程文件是否存在，如果存在则给出提示：进程已存在，请先停止
  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi
	
	# 如果spark_master不为空，则在masters(master高可用的情况)中同步删除部分文件
  if [ "$SPARK_MASTER" != "" ]; then
    echo rsync from "$SPARK_MASTER"
    rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' "$SPARK_MASTER/" "${SPARK_HOME}"
  fi

  spark_rotate_log "$log"
  echo "starting $command, logging to $log"

  case "$mode" in
    (class)
      execute_command nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class "$command" "$@"
      ;;

    (submit)
      execute_command nice -n "$SPARK_NICENESS" bash "${SPARK_HOME}"/bin/spark-submit --class "$command" "$@"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac
}

case $option in

  (submit)
    run_command submit "$@"
    ;;

  (start)
    run_command class "$@"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;

  (decommission)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "decommissioning $command"
        kill -s SIGPWR "$TARGET_ID"
      else
        echo "no $command to decommission"
      fi
    else
      echo "no $command to decommission"
    fi
    ;;

  (status)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo $command is running.
        exit 0
      else
        echo $pid file is present but $command not running
        exit 1
      fi
    else
      echo $command not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
```

