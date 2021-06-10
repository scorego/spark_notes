> 使用spark-submit脚本提交Spark作业时最终会来到`org.apache.spark.deploy.SparkSubmit`类，该类负责提交作业、查询状态等操作。那么具体流程是什么呢？
>
> 还以上篇中的脚本命令为例说明：
>
> ```bash
> spark-submit脚本命令：
> ./bin/spark-submit \
>     --class com.example.WordCount \
>     --master spark://127.0.0.1:7077 \
>     --deploy-mode cluster \
>     --executor-memory 4G \
>     ./user-jar-0.1.1.jar arg1 arg2
> 
> 实际上相当于调用spark-class脚本并执行命令：
> java -Xmx128m -cp ...jars org.apache.spark.launcher.Main \
>     org.apache.spark.deploy.SparkSubmit \
>     --class com.example.WordCount \
>     --master spark://127.0.0.1:7077 \
>     --deploy-mode cluster \
>     --executor-memory 4G \
>     ./user-jar-0.1.1.jar arg1 arg2
> 
> 最终执行的命令是：
> java -cp ... -Duser.home=/home/work  org.apache.spark.deploy.SparkSubmit \
> 	--master spark://127.0.0.1:7077 \
> 	--deploy-mode cluster \
> 	--class com.example.WordCount \
> 	--executor-memory 4G \
> 	./user-jar-0.1.1.jar arg1 arg2
> ```
>
> 

---

`org.apache.spark.deploy.SparkSubmit`类定义如下：

```scala
package org.apache.spark.deploy

/**
 * Main gateway of launching a Spark application. 该类是启动Spark应用程序的主要入口。
 *
 * This program handles setting up the classpath with relevant Spark dependencies and provides
 * a layer over the different cluster managers and deploy modes that Spark supports.
 */
private[spark] class SparkSubmit extends Logging  {...}

object SparkSubmit extends CommandLineUtils with Logging {...}
```

可以看到，`SparkSubmit`类继承了`CommandLineUtils`类，后者实现了`CommandLineLoggingUtils`特质，从而有了`exitFn`方法。`exitFn`的逻辑就是按一定的退出码退出程序。

```scala
/**
 * Contains basic command line parsing functionality and methods to parse some common Spark CLI options.
 */
private[spark] trait CommandLineUtils extends CommandLineLoggingUtils {
  def main(args: Array[String]): Unit
}

private[spark] trait CommandLineLoggingUtils {
  // Exposed for testing
  private[spark] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)

  private[spark] var printStream: PrintStream = System.err

  // scalastyle:off println
  private[spark] def printMessage(str: String): Unit = printStream.println(str)
  // scalastyle:on println

  private[spark] def printErrorAndExit(str: String): Unit = {
    printMessage("Error: " + str)
    printMessage("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }
}
```

# 一、 入口：`main()`方法

`org.apache.spark.deploy.SparkSubmit`类是启动Spark应用程序的主要入口，`main`方法如下：

```scala
override def main(args: Array[String]): Unit = {
    // 1. 新建一个SparkSubmit类实例
    val submit = new SparkSubmit() {
        self =>

        override protected def parseArguments(args: Array[String]): SparkSubmitArguments = {
            new SparkSubmitArguments(args) {
                override protected def logInfo(msg: => String): Unit = self.logInfo(msg)

                override protected def logWarning(msg: => String): Unit = self.logWarning(msg)

                override protected def logError(msg: => String): Unit = self.logError(msg)
            }
        }

        override protected def logInfo(msg: => String): Unit = printMessage(msg)

        override protected def logWarning(msg: => String): Unit = printMessage(s"Warning: $msg")

        override protected def logError(msg: => String): Unit = printMessage(s"Error: $msg")

        override def doSubmit(args: Array[String]): Unit = {
            try {
                super.doSubmit(args)
            } catch {
                case e: SparkUserAppException =>
                exitFn(e.exitCode)
            }
        }
    }

    // 2. 调用该实例的doSubmit方法
    submit.doSubmit(args)
}
```

可以看到，`main`方法就是新建了`org.apache.spark.deploy.SparkSubmit.SparkSubmit`类(自己)的实例，然后在实例上调用`doSubmit`方法。

# 二、`doSubmit()`

`main()`方法新建`SparkSubmit`实例时重写了`doSubmit()`/`parseArguments()`等方法，重写`parseArguments`主要和日志打印有关，而重写`doSubmit`方法主要是多了捕捉异常并退出程序这一步骤。

```scala
override def doSubmit(args: Array[String]): Unit = {
  try {
    super.doSubmit(args)
  } catch {
    case e: SparkUserAppException => exitFn(e.exitCode)
  }
}
```

来看`super.doSubmit(args)`这里，`private[spark] class SparkSubmit`的`main`方法主要分三步，分别是初始化日志、解析参数、执行动作：

```scala
def doSubmit(args: Array[String]): Unit = {
    // 1. 初始化日志
    val uninitLog = initializeLogIfNecessary(true, silent = true)
	
    // 2. 解析参数
    val appArgs = parseArguments(args)
    if (appArgs.verbose) {
    	logInfo(appArgs.toString)
    }
    
    // 3. 执行动作
    appArgs.action match {
        case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
        case SparkSubmitAction.KILL => kill(appArgs)
        case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
        case SparkSubmitAction.PRINT_VERSION => printVersion()
    }
}
```

## 1. 初始化日志

如果日志还没初始化，那么初始化日志，并判断是否需要在application启动前reset日志。

```scala
protected def initializeLogIfNecessary(isInterpreter: Boolean,
      								   silent: Boolean = false): Boolean = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging(isInterpreter, silent)
          return true
        }
      }
    }
    false
}

private def initializeLogging(isInterpreter: Boolean, silent: Boolean): Unit = {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    if (Logging.isLog4j12()) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        Logging.defaultSparkLog4jConfig = true
        val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
        Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            if (!silent) {
              System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
            }
          case None =>
            System.err.println(s"Spark was unable to load $defaultLogProps")
        }
      }

      val rootLogger = LogManager.getRootLogger()
      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel()
      }

      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val replLogger = LogManager.getLogger(logName)
        val replLevel = Option(replLogger.getLevel()).getOrElse(Level.WARN)
        // Update the consoleAppender threshold to replLevel
        if (replLevel != rootLogger.getEffectiveLevel()) {
          if (!silent) {
            System.err.printf("Setting default log level to \"%s\".\n", replLevel)
            System.err.println("To adjust logging level use sc.setLogLevel(newLevel). " +
              "For SparkR, use setLogLevel(newLevel).")
          }
          Logging.sparkShellThresholdLevel = replLevel
          rootLogger.getAllAppenders().asScala.foreach {
            case ca: ConsoleAppender =>
              ca.addFilter(new SparkShellLoggingFilter())
            case _ => // no-op
          }
        }
      }
      // scalastyle:on println
    }
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
}
```

## 2. 解析参数

第二步是解析参数。

```scala
val appArgs = parseArguments(args)
if (appArgs.verbose) {
    logInfo(appArgs.toString)
}
```

参数形如：

```shell
java -cp ... -Duser.home=/home/work  org.apache.spark.deploy.SparkSubmit --master spark://127.0.0.1:7077 --deploy-mode cluster --class com.example.WordCount --executor-memory 4G ./user-jar-0.1.1.jar arg1 arg2
```

`parseArguments`这个方法是在实例化`SparkSubmit`类的时候被重写过的，不过也就是更改打印日志的一些细节，主要还是通过新建`org.apache.spark.deploy.SparkSubmitArguments`类实例来解析参数：

```scala
override protected def parseArguments(args: Array[String]): SparkSubmitArguments = {
    new SparkSubmitArguments(args) {
        override protected def logInfo(msg: => String): Unit = self.logInfo(msg)
        override protected def logWarning(msg: => String): Unit = self.logWarning(msg)
        override protected def logError(msg: => String): Unit = self.logError(msg)
    }
}
```

`org.apache.spark.deploy.SparkSubmitArguments`类继承了`org.apache.spark.launcher.SparkSubmitArgumentsParser`类，并调用`parse`方法来解析参数：

```scala
/**
 * Parses and encapsulates arguments from the spark-submit script.
 * The env argument is used for testing.
 */
private[deploy] class SparkSubmitArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends SparkSubmitArgumentsParser with Logging {
  
  ...
      
  // 解析参数
  parse(args.asJava)

  // 从properties file中填充`sparkProperties`所含的键值对映射
  mergeDefaultSparkProperties()
  // 从`sparkProperties`中移除不以`spark.`开头的key
  ignoreNonSparkProperties()
  // 使用`sparkProperties`和env变量来填充任何缺少的参数
  loadEnvironmentArguments()

  useRest = sparkProperties.getOrElse("spark.master.rest.enabled", "false").toBoolean

  // 验证参数是完整、有效
  validateArguments()
}
```

`parse()`解析的参数会把对应的值赋给类的实例变量，且第一个无法识别的参数会被认定为`primaryResource: String`变量，之后的变量被认定为应用程序的参数添加到`childArgs: ArrayBuffer[String]`中。

## 3. 执行动作

根据上步解析的`action`来执行不同的动作：

```scala
appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
      case SparkSubmitAction.PRINT_VERSION => printVersion()
}

/**
 * Whether to submit, kill, or request the status of an application.
 * The latter two operations are currently supported only for standalone and Mesos cluster modes.
 */
private[deploy] object SparkSubmitAction extends Enumeration {
  type SparkSubmitAction = Value
  val SUBMIT, KILL, REQUEST_STATUS, PRINT_VERSION = Value
}
```

如果是提交作业，则会匹配到`submit()`方法；如果是kill作业，则会匹配到`kill()`方法；`requestStatus()`和`PRINT_VERSION()`则只能在standalone和Mesos模式使用。

# 二、submit方法

`submit()`方法是提交Spark作业，其实主要就是调用`runMain()`方法。`runMain()`使用参数来运行子类(子类就是脚本最终解析的命令中用`--class`指定的类)的`main()`方法，共两步：

1. 根据不同的cluster manager和deploy mode来准备启动环境，诸如classpath、系统变量、application参数
2. 使用该启动环境来调用子类的main方法

```scala
/**
   * Run the main method of the child class using the submit arguments.
   *
   * This runs in two steps. First, we prepare the launch environment by setting up
   * the appropriate classpath, system properties, and application arguments for
   * running the child main class based on the cluster manager and the deploy mode.
   * Second, we use this launch environment to invoke the main method of the child
   * main class.
   *
   * Note that this main class will not be the one provided by the user if we're
   * running cluster deploy mode or python applications.
   */
private def runMain(args: SparkSubmitArguments, uninitLog: Boolean): Unit = {
    // 准备环境
    val (childArgs, childClasspath, sparkConf, childMainClass) = prepareSubmitEnvironment(args)

    // Let the main class re-initialize the logging system once it starts.
    // 启动main后重新初始化logging
    if (uninitLog) {
        Logging.uninitialize()
    }

    if (args.verbose) {
        logInfo(s"Main class:\n$childMainClass")
        logInfo(s"Arguments:\n${childArgs.mkString("\n")}")
        // sysProps may contain sensitive information, so redact before printing
        logInfo(s"Spark config:\n${Utils.redact(sparkConf.getAll.toMap).mkString("\n")}")
        logInfo(s"Classpath elements:\n${childClasspath.mkString("\n")}")
        logInfo("\n")
    }

    //
    val loader = getSubmitClassLoader(sparkConf)
    for (jar <- childClasspath) {
        addJarToClasspath(jar, loader)
    }

    // 
    var mainClass: Class[_] = null
    try {
        mainClass = Utils.classForName(childMainClass)
    } catch {
            case e: ClassNotFoundException =>
            logError(s"Failed to load class $childMainClass.")
            if (childMainClass.contains("thriftserver")) {
                logInfo(s"Failed to load main class $childMainClass.")
                logInfo("You need to build Spark with -Phive and -Phive-thriftserver.")
            }
            throw new SparkUserAppException(CLASS_NOT_FOUND_EXIT_STATUS)
        case e: NoClassDefFoundError =>
            logError(s"Failed to load $childMainClass: ${e.getMessage()}")
            if (e.getMessage.contains("org/apache/hadoop/hive")) {
                logInfo(s"Failed to load hive class.")
                logInfo("You need to build Spark with -Phive and -Phive-thriftserver.")
            }
            throw new SparkUserAppException(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    val app: SparkApplication = if (classOf[SparkApplication].isAssignableFrom(mainClass)) {
        mainClass.getConstructor().newInstance().asInstanceOf[SparkApplication]
    } else {
        new JavaMainApplication(mainClass)
    }

    @tailrec
    def findCause(t: Throwable): Throwable = t match {
        case e: UndeclaredThrowableException =>
        	if (e.getCause() != null) findCause(e.getCause()) else e
        case e: InvocationTargetException =>
        	if (e.getCause() != null) findCause(e.getCause()) else e
        case e: Throwable => e
    }

    try {
        app.start(childArgs.toArray, sparkConf)
    } catch {
        case t: Throwable =>
        throw findCause(t)
    } finally {
        if (!isShell(args.primaryResource) && !isSqlShell(args.mainClass) && !isThriftServer(args.mainClass)) {
            try {
                SparkContext.getActive.foreach(_.stop())
            } catch {
                case e: Throwable => logError(s"Failed to close SparkContext: $e")
            }
        }
    }
}
```

## 1. 准备环境：prepareSubmitEnvironment()

```scala
  val (childArgs, childClasspath, sparkConf, childMainClass) = prepareSubmitEnvironment(args)

  /**
   * Prepare the environment for submitting an application.
   *
   * @param args the parsed SparkSubmitArguments used for environment preparation.
   * @param conf the Hadoop Configuration, this argument will only be set in unit test.
   * @return a 4-tuple:
   *        (1) the arguments for the child process,
   *        (2) a list of classpath entries for the child,
   *        (3) a map of system properties, and
   *        (4) the main class for the child
   *
   * Exposed for testing.
   */
  private[deploy] def prepareSubmitEnvironment(
      args: SparkSubmitArguments,
      conf: Option[HadoopConfiguration] = None)
      : (Seq[String], Seq[String], SparkConf, String) = {
    // Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val sparkConf = args.toSparkConf()
    var childMainClass = ""

    // Set the cluster manager
    val clusterManager: Int = args.master match {
      case "yarn" => YARN
      case m if m.startsWith("spark") => STANDALONE
      case m if m.startsWith("mesos") => MESOS
      case m if m.startsWith("k8s") => KUBERNETES
      case m if m.startsWith("local") => LOCAL
      case _ =>
        error("Master must either be yarn or start with spark, mesos, k8s, or local")
        -1
    }

    // Set the deploy mode; default is client mode
    var deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT
      case "cluster" => CLUSTER
      case _ =>
        error("Deploy mode must be either client or cluster")
        -1
    }

    if (clusterManager == YARN) {
      // Make sure YARN is included in our build if we're trying to use it
      if (!Utils.classIsLoadable(YARN_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        error(
          "Could not load YARN classes. This copy of Spark may not have been compiled with YARN support.")
      }
    }
    if (clusterManager == KUBERNETES) {
      args.master = Utils.checkAndGetK8sMasterUrl(args.master)
      // Make sure KUBERNETES is included in our build if we're trying to use it
      if (!Utils.classIsLoadable(KUBERNETES_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        error("Could not load KUBERNETES classes. This copy of Spark may not have been compiled with KUBERNETES support.")
      }
    }

    // Fail fast, the following modes are not supported or applicable
    (clusterManager, deployMode) match {
      case (STANDALONE, CLUSTER) if args.isPython =>
        error("Cluster deploy mode is currently not supported for python applications on standalone clusters.")
      case (STANDALONE, CLUSTER) if args.isR =>
        error("Cluster deploy mode is currently not supported for R applications on standalone clusters.")
      case (LOCAL, CLUSTER) =>
        error("Cluster deploy mode is not compatible with master \"local\"")
      case (_, CLUSTER) if isShell(args.primaryResource) =>
        error("Cluster deploy mode is not applicable to Spark shells.")
      case (_, CLUSTER) if isSqlShell(args.mainClass) =>
        error("Cluster deploy mode is not applicable to Spark SQL shell.")
      case (_, CLUSTER) if isThriftServer(args.mainClass) =>
        error("Cluster deploy mode is not applicable to Spark Thrift server.")
      case _ =>
    }

    // Update args.deployMode if it is null. It will be passed down as a Spark property later.
    (args.deployMode, deployMode) match {
      case (null, CLIENT) => args.deployMode = "client"
      case (null, CLUSTER) => args.deployMode = "cluster"
      case _ =>
    }
    val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER
    val isMesosCluster = clusterManager == MESOS && deployMode == CLUSTER
    val isStandAloneCluster = clusterManager == STANDALONE && deployMode == CLUSTER
    val isKubernetesCluster = clusterManager == KUBERNETES && deployMode == CLUSTER
    val isKubernetesClient = clusterManager == KUBERNETES && deployMode == CLIENT
    val isKubernetesClusterModeDriver = isKubernetesClient && 
        		sparkConf.getBoolean("spark.kubernetes.submitInDriver", false)

    if (!isMesosCluster && !isStandAloneCluster) {
      // Resolve maven dependencies if there are any and add classpath to jars. Add them to py-files
      // too for packages that include Python code
      val resolvedMavenCoordinates = DependencyUtils.resolveMavenDependencies(
        args.packagesExclusions, args.packages, args.repositories, args.ivyRepoPath,
        args.ivySettingsPath)

      if (!StringUtils.isBlank(resolvedMavenCoordinates)) {
        // In K8s client mode, when in the driver, add resolved jars early as we might need
        // them at the submit time for artifact downloading.
        // For example we might use the dependencies for downloading
        // files from a Hadoop Compatible fs e.g. S3. In this case the user might pass:
        // --packages com.amazonaws:aws-java-sdk:1.7.4:org.apache.hadoop:hadoop-aws:2.7.6
        if (isKubernetesClusterModeDriver) {
          val loader = getSubmitClassLoader(sparkConf)
          for (jar <- resolvedMavenCoordinates.split(",")) {
            addJarToClasspath(jar, loader)
          }
        } else if (isKubernetesCluster) {
          // We need this in K8s cluster mode so that we can upload local deps
          // via the k8s application, like in cluster mode driver
          childClasspath ++= resolvedMavenCoordinates.split(",")
        } else {
          args.jars = mergeFileLists(args.jars, resolvedMavenCoordinates)
          if (args.isPython || isInternal(args.primaryResource)) {
            args.pyFiles = mergeFileLists(args.pyFiles, resolvedMavenCoordinates)
          }
        }
      }

      // install any R packages that may have been passed through --jars or --packages.
      // Spark Packages may contain R source code inside the jar.
      if (args.isR && !StringUtils.isBlank(args.jars)) {
        RPackageUtils.checkAndBuildRPackage(args.jars, printStream, args.verbose)
      }
    }

    // update spark config from args
    args.toSparkConf(Option(sparkConf))
    val hadoopConf = conf.getOrElse(SparkHadoopUtil.newConfiguration(sparkConf))
    val targetDir = Utils.createTempDir()

    // Kerberos is not supported in standalone mode, and keytab support is not yet available
    // in Mesos cluster mode.
    if (clusterManager != STANDALONE
        && !isMesosCluster
        && args.principal != null
        && args.keytab != null) {
      // If client mode, make sure the keytab is just a local path.
      if (deployMode == CLIENT && Utils.isLocalUri(args.keytab)) {
        args.keytab = new URI(args.keytab).getPath()
      }

      if (!Utils.isLocalUri(args.keytab)) {
        require(new File(args.keytab).exists(), s"Keytab file: ${args.keytab} does not exist")
        UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
      }
    }

    // Resolve glob path for different resources.
    args.jars = Option(args.jars).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.files = Option(args.files).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.pyFiles = Option(args.pyFiles).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.archives = Option(args.archives).map(resolveGlobPaths(_, hadoopConf)).orNull

    lazy val secMgr = new SecurityManager(sparkConf)

    // In client mode, download remote files.
    var localPrimaryResource: String = null
    var localJars: String = null
    var localPyFiles: String = null
    if (deployMode == CLIENT) {
      localPrimaryResource = Option(args.primaryResource).map {
        downloadFile(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull
      localJars = Option(args.jars).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull
      localPyFiles = Option(args.pyFiles).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull

      if (isKubernetesClusterModeDriver) {
        // Replace with the downloaded local jar path to avoid propagating hadoop compatible uris.
        // Executors will get the jars from the Spark file server.
        // Explicitly download the related files here
        args.jars = localJars
        val filesLocalFiles = Option(args.files).map {
          downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr)
        }.orNull
        val archiveLocalFiles = Option(args.archives).map { uris =>
          val resolvedUris = Utils.stringToSeq(uris).map(Utils.resolveURI)
          val localArchives = downloadFileList(
            resolvedUris.map(
              UriBuilder.fromUri(_).fragment(null).build().toString).mkString(","),
            targetDir, sparkConf, hadoopConf, secMgr)

          // SPARK-33748: this mimics the behaviour of Yarn cluster mode. If the driver is running
          // in cluster mode, the archives should be available in the driver's current working
          // directory too.
          Utils.stringToSeq(localArchives).map(Utils.resolveURI).zip(resolvedUris).map {
            case (localArchive, resolvedUri) =>
              val source = new File(localArchive.getPath)
              val dest = new File(
                ".",
                if (resolvedUri.getFragment != null) resolvedUri.getFragment else source.getName)
              logInfo(
                s"Unpacking an archive $resolvedUri " +
                  s"from ${source.getAbsolutePath} to ${dest.getAbsolutePath}")
              Utils.deleteRecursively(dest)
              Utils.unpack(source, dest)

              // Keep the URIs of local files with the given fragments.
              UriBuilder.fromUri(
                localArchive).fragment(resolvedUri.getFragment).build().toString
          }.mkString(",")
        }.orNull
        args.files = filesLocalFiles
        args.archives = archiveLocalFiles
        args.pyFiles = localPyFiles
      }
    }
	
    // When running in YARN, for some remote resources with scheme:
    //   1. Hadoop FileSystem doesn't support them.
    //   2. We explicitly bypass Hadoop FileSystem with "spark.yarn.dist.forceDownloadSchemes".
    // We will download them to local disk prior to add to YARN's distributed cache.
    // For yarn client mode, since we already download them with above code, so we only need to
    // figure out the local path and replace the remote one.
    // yarn模式下载资源
    if (clusterManager == YARN) {
      // 加载方案列表
      val forceDownloadSchemes = sparkConf.get(FORCE_DOWNLOAD_SCHEMES)
	
      // 判断是否需要下载的方法
      def shouldDownload(scheme: String): Boolean = {
        forceDownloadSchemes.contains("*") || forceDownloadSchemes.contains(scheme) ||
          Try { FileSystem.getFileSystemClass(scheme, hadoopConf) }.isFailure
      }
        
      // 下载资源的方法
      def downloadResource(resource: String): String = {
        val uri = Utils.resolveURI(resource)
        uri.getScheme match {
          case "local" | "file" => resource
          case e if shouldDownload(e) =>
            val file = new File(targetDir, new Path(uri).getName)
            if (file.exists()) {
              file.toURI.toString
            } else {
              downloadFile(resource, targetDir, sparkConf, hadoopConf, secMgr)
            }
          case _ => uri.toString
        }
      }
        
      // 下载主要运行资源
      args.primaryResource = Option(args.primaryResource).map { downloadResource }.orNull
      
      // 下载文件
      args.files = Option(args.files).map { files =>
        Utils.stringToSeq(files).map(downloadResource).mkString(",")
      }.orNull
      args.pyFiles = Option(args.pyFiles).map { pyFiles =>
        Utils.stringToSeq(pyFiles).map(downloadResource).mkString(",")
      }.orNull
        
      // 下载jars
      args.jars = Option(args.jars).map { jars =>
        Utils.stringToSeq(jars).map(downloadResource).mkString(",")
      }.orNull
        
      // 下载压缩文件
      args.archives = Option(args.archives).map { archives =>
        Utils.stringToSeq(archives).map(downloadResource).mkString(",")
      }.orNull
    }

    // At this point, we have attempted to download all remote resources.
    // Now we try to resolve the main class if our primary resource is a JAR.
    // 如果主类未设置，尝试从JAR包中找到主类
    if (args.mainClass == null && !args.isPython && !args.isR) {
      try {
        val uri = new URI(
          Option(localPrimaryResource).getOrElse(args.primaryResource)
        )
        val fs = FileSystem.get(uri, hadoopConf)

        Utils.tryWithResource(new JarInputStream(fs.open(new Path(uri)))) { jar =>
          args.mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
        }
      } catch {
        case e: Throwable =>
          error(
            s"Failed to get main class in JAR with error '${e.getMessage}'. " +
            " Please specify one with --class."
          )
      }

      if (args.mainClass == null) {
        // If we still can't figure out the main class at this point, blow up.
        error("No main class set in JAR; please specify one with --class.")
      }
    }

    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython && deployMode == CLIENT) {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "org.apache.spark.api.python.PythonGatewayServer"
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource, localPyFiles) ++ args.childArgs
      }
    }

    // Non-PySpark applications can need Python dependencies.
    if (deployMode == CLIENT && clusterManager != YARN) {
      // The YARN backend handles python files differently, so don't merge the lists.
      args.files = mergeFileLists(args.files, args.pyFiles)
    }

    if (localPyFiles != null) {
      sparkConf.set(SUBMIT_PYTHON_FILES, localPyFiles.split(",").toSeq)
    }

    // In YARN mode for an R app, add the SparkR package archive and the R package
    // archive containing all of the built R libraries to archives so that they can
    // be distributed with the job
    if (args.isR && clusterManager == YARN) {
      val sparkRPackagePath = RUtils.localSparkRPackagePath
      if (sparkRPackagePath.isEmpty) {
        error("SPARK_HOME does not exist for R application in YARN mode.")
      }
      val sparkRPackageFile = new File(sparkRPackagePath.get, SPARKR_PACKAGE_ARCHIVE)
      if (!sparkRPackageFile.exists()) {
        error(s"$SPARKR_PACKAGE_ARCHIVE does not exist for R application in YARN mode.")
      }
      val sparkRPackageURI = Utils.resolveURI(sparkRPackageFile.getAbsolutePath).toString

      // Distribute the SparkR package.
      // Assigns a symbol link name "sparkr" to the shipped package.
      args.archives = mergeFileLists(args.archives, sparkRPackageURI + "#sparkr")

      // Distribute the R package archive containing all the built R packages.
      if (!RUtils.rPackages.isEmpty) {
        val rPackageFile =
          RPackageUtils.zipRLibraries(new File(RUtils.rPackages.get), R_PACKAGE_ARCHIVE)
        if (!rPackageFile.exists()) {
          error("Failed to zip all the built R packages.")
        }

        val rPackageURI = Utils.resolveURI(rPackageFile.getAbsolutePath).toString
        // Assigns a symbol link name "rpkg" to the shipped package.
        args.archives = mergeFileLists(args.archives, rPackageURI + "#rpkg")
      }
    }

    // TODO: Support distributing R packages with standalone cluster
    if (args.isR && clusterManager == STANDALONE && !RUtils.rPackages.isEmpty) {
      error("Distributing R packages with standalone cluster is not supported.")
    }

    // TODO: Support distributing R packages with mesos cluster
    if (args.isR && clusterManager == MESOS && !RUtils.rPackages.isEmpty) {
      error("Distributing R packages with mesos cluster is not supported.")
    }

    // If we're running an R app, set the main class to our specific R runner
    if (args.isR && deployMode == CLIENT) {
      if (args.primaryResource == SPARKR_SHELL) {
        args.mainClass = "org.apache.spark.api.r.RBackend"
      } else {
        // If an R file is provided, add it to the child arguments and list of files to deploy.
        // Usage: RRunner <main R file> [app arguments]
        args.mainClass = "org.apache.spark.deploy.RRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource) ++ args.childArgs
        args.files = mergeFileLists(args.files, args.primaryResource)
      }
    }
	
    // yarncluster模式的R代码，将主要运行资源合并到文件列表中,随job运行一起分发
    if (isYarnCluster && args.isR) {
      // In yarn-cluster mode for an R app, add primary resource to files
      // that can be distributed with the job
      args.files = mergeFileLists(args.files, args.primaryResource)
    }

    // Special flag to avoid deprecation warnings at the client
    sys.props("SPARK_SUBMIT") = "true"

    // A list of rules to map each argument to system properties or command-line options in
    // each deploy mode; we iterate through these below
    val options = List[OptionAssigner](

      // All cluster managers，所有集群管理的参数属性
      OptionAssigner(args.master, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.master"),
      OptionAssigner(args.deployMode, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = SUBMIT_DEPLOY_MODE.key),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.app.name"),
      OptionAssigner(args.ivyRepoPath, ALL_CLUSTER_MGRS, CLIENT, confKey = "spark.jars.ivy"),
      OptionAssigner(args.driverMemory, ALL_CLUSTER_MGRS, CLIENT,
        confKey = DRIVER_MEMORY.key),
      OptionAssigner(args.driverExtraClassPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = DRIVER_CLASS_PATH.key),
      OptionAssigner(args.driverExtraJavaOptions, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = DRIVER_JAVA_OPTIONS.key),
      OptionAssigner(args.driverExtraLibraryPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = DRIVER_LIBRARY_PATH.key),
      OptionAssigner(args.principal, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = PRINCIPAL.key),
      OptionAssigner(args.keytab, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = KEYTAB.key),
      OptionAssigner(args.pyFiles, ALL_CLUSTER_MGRS, CLUSTER, confKey = SUBMIT_PYTHON_FILES.key),

      // Propagate attributes for dependency resolution at the driver side
      OptionAssigner(args.packages, STANDALONE | MESOS | KUBERNETES,
        CLUSTER, confKey = "spark.jars.packages"),
      OptionAssigner(args.repositories, STANDALONE | MESOS | KUBERNETES,
        CLUSTER, confKey = "spark.jars.repositories"),
      OptionAssigner(args.ivyRepoPath, STANDALONE | MESOS | KUBERNETES,
        CLUSTER, confKey = "spark.jars.ivy"),
      OptionAssigner(args.packagesExclusions, STANDALONE | MESOS | KUBERNETES,
        CLUSTER, confKey = "spark.jars.excludes"),

      // Yarn only,yarn模式时需要的参数属性,包括队列、依赖包和文件等
      OptionAssigner(args.queue, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.queue"),
      OptionAssigner(args.pyFiles, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.pyFiles",
        mergeFn = Some(mergeFileLists(_, _))),
      OptionAssigner(args.jars, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.jars",
        mergeFn = Some(mergeFileLists(_, _))),
      OptionAssigner(args.files, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.files",
        mergeFn = Some(mergeFileLists(_, _))),
      OptionAssigner(args.archives, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.archives",
        mergeFn = Some(mergeFileLists(_, _))),

      // Other options
      OptionAssigner(args.numExecutors, YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = EXECUTOR_INSTANCES.key),
      OptionAssigner(args.executorCores, STANDALONE | YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = EXECUTOR_CORES.key),
      OptionAssigner(args.executorMemory, STANDALONE | MESOS | YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = EXECUTOR_MEMORY.key),
      OptionAssigner(args.totalExecutorCores, STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = CORES_MAX.key),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = FILES.key),
      OptionAssigner(args.archives, LOCAL | STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = ARCHIVES.key),
      OptionAssigner(args.jars, LOCAL, CLIENT, confKey = JARS.key),
      OptionAssigner(args.jars, STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = JARS.key),
      OptionAssigner(args.driverMemory, STANDALONE | MESOS | YARN | KUBERNETES, CLUSTER,
        confKey = DRIVER_MEMORY.key),
      OptionAssigner(args.driverCores, STANDALONE | MESOS | YARN | KUBERNETES, CLUSTER,
        confKey = DRIVER_CORES.key),
      OptionAssigner(args.supervise.toString, STANDALONE | MESOS, CLUSTER,
        confKey = DRIVER_SUPERVISE.key),
      OptionAssigner(args.ivyRepoPath, STANDALONE, CLUSTER, confKey = "spark.jars.ivy"),

      // An internal option used only for spark-shell to add user jars to repl's classloader,
      // previously it uses "spark.jars" or "spark.yarn.dist.jars" which now may be pointed to
      // remote jars, so adding a new option to only specify local jars for spark-shell internally.
      OptionAssigner(localJars, ALL_CLUSTER_MGRS, CLIENT, confKey = "spark.repl.local.jars")
    )

    // In client mode, launch the application main class directly
    // In addition, add the main application jar and any added jars (if any) to the classpath
    if (deployMode == CLIENT) {
      childMainClass = args.mainClass
      if (localPrimaryResource != null && isUserJar(localPrimaryResource)) {
        childClasspath += localPrimaryResource
      }
      if (localJars != null) { childClasspath ++= localJars.split(",") }
    }
    // Add the main application jar and any added jars to classpath in case YARN client
    // requires these jars.
    // This assumes both primaryResource and user jars are local jars, or already downloaded
    // to local by configuring "spark.yarn.dist.forceDownloadSchemes", otherwise it will not be
    // added to the classpath of YARN client.
    if (isYarnCluster) {
      if (isUserJar(args.primaryResource)) {
        childClasspath += args.primaryResource
      }
      if (args.jars != null) { childClasspath ++= args.jars.split(",") }
    }

    if (deployMode == CLIENT) {
      if (args.childArgs != null) { childArgs ++= args.childArgs }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    for (opt <- options) {
      if (opt.value != null &&
          (deployMode & opt.deployMode) != 0 &&
          (clusterManager & opt.clusterManager) != 0) {
        if (opt.clOption != null) { childArgs += (opt.clOption, opt.value) }
        if (opt.confKey != null) {
          if (opt.mergeFn.isDefined && sparkConf.contains(opt.confKey)) {
            sparkConf.set(opt.confKey, opt.mergeFn.get.apply(sparkConf.get(opt.confKey), opt.value))
          } else {
            sparkConf.set(opt.confKey, opt.value)
          }
        }
      }
    }

    // In case of shells, spark.ui.showConsoleProgress can be true by default or by user.
    if (isShell(args.primaryResource) && !sparkConf.contains(UI_SHOW_CONSOLE_PROGRESS)) {
      sparkConf.set(UI_SHOW_CONSOLE_PROGRESS, true)
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python and R files, the primary resource is already distributed as a regular file
    if (!isYarnCluster && !args.isPython && !args.isR) {
      var jars = sparkConf.get(JARS)
      if (isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sparkConf.set(JARS, jars)
    }

    // In standalone cluster mode, use the REST client to submit the application (Spark 1.3+).
    // All Spark parameters are expected to be passed to the client through system properties.
    if (args.isStandaloneCluster) {
      if (args.useRest) {
        childMainClass = REST_CLUSTER_SUBMIT_CLASS
        childArgs += (args.primaryResource, args.mainClass)
      } else {
        // In legacy standalone cluster mode, use Client as a wrapper around the user class
        childMainClass = STANDALONE_CLUSTER_SUBMIT_CLASS
        if (args.supervise) { childArgs += "--supervise" }
        Option(args.driverMemory).foreach { m => childArgs += ("--memory", m) }
        Option(args.driverCores).foreach { c => childArgs += ("--cores", c) }
        childArgs += "launch"
        childArgs += (args.master, args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // Let YARN know it's a pyspark app, so it distributes needed libraries.
    if (clusterManager == YARN) {
      if (args.isPython) {
        sparkConf.set("spark.yarn.isPython", "true")
      }
    }

    if ((clusterManager == MESOS || clusterManager == KUBERNETES)
       && UserGroupInformation.isSecurityEnabled) {
      setRMPrincipal(sparkConf)
    }

    // In yarn-cluster mode, use yarn.Client as a wrapper around the user class
    if (isYarnCluster) {
      childMainClass = YARN_CLUSTER_SUBMIT_CLASS
      if (args.isPython) {
        childArgs += ("--primary-py-file", args.primaryResource)
        childArgs += ("--class", "org.apache.spark.deploy.PythonRunner")
      } else if (args.isR) {
        val mainFile = new Path(args.primaryResource).getName
        childArgs += ("--primary-r-file", mainFile)
        childArgs += ("--class", "org.apache.spark.deploy.RRunner")
      } else {
        if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
          childArgs += ("--jar", args.primaryResource)
        }
        childArgs += ("--class", args.mainClass)
      }
      if (args.childArgs != null) {
        args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
      }
    }

    if (isMesosCluster) {
      assert(args.useRest, "Mesos cluster mode is only supported through the REST submission API")
      childMainClass = REST_CLUSTER_SUBMIT_CLASS
      if (args.isPython) {
        // Second argument is main class
        childArgs += (args.primaryResource, "")
        if (args.pyFiles != null) {
          sparkConf.set(SUBMIT_PYTHON_FILES, args.pyFiles.split(",").toSeq)
        }
      } else if (args.isR) {
        // Second argument is main class
        childArgs += (args.primaryResource, "")
      } else {
        childArgs += (args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    if (isKubernetesCluster) {
      childMainClass = KUBERNETES_CLUSTER_SUBMIT_CLASS
      if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
        if (args.isPython) {
          childArgs ++= Array("--primary-py-file", args.primaryResource)
          childArgs ++= Array("--main-class", "org.apache.spark.deploy.PythonRunner")
        } else if (args.isR) {
          childArgs ++= Array("--primary-r-file", args.primaryResource)
          childArgs ++= Array("--main-class", "org.apache.spark.deploy.RRunner")
        }
        else {
          childArgs ++= Array("--primary-java-resource", args.primaryResource)
          childArgs ++= Array("--main-class", args.mainClass)
        }
      } else {
        childArgs ++= Array("--main-class", args.mainClass)
      }
      if (args.childArgs != null) {
        args.childArgs.foreach { arg =>
          childArgs += ("--arg", arg)
        }
      }
      // Pass the proxyUser to the k8s app so it is possible to add it to the driver args
      if (args.proxyUser != null) {
        childArgs += ("--proxy-user", args.proxyUser)
      }
    }

    // Load any properties specified through --conf and the default properties file
    for ((k, v) <- args.sparkProperties) {
      sparkConf.setIfMissing(k, v)
    }

    // Ignore invalid spark.driver.host in cluster modes.
    if (deployMode == CLUSTER) {
      sparkConf.remove(DRIVER_HOST_ADDRESS)
    }

    // Resolve paths in certain spark properties
    val pathConfigs = Seq(
      JARS.key,
      FILES.key,
      ARCHIVES.key,
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives",
      "spark.yarn.dist.jars")
    pathConfigs.foreach { config =>
      // Replace old URIs with resolved URIs, if they exist
      sparkConf.getOption(config).foreach { oldValue =>
        sparkConf.set(config, Utils.resolveURIs(oldValue))
      }
    }

    // Resolve and format python file paths properly before adding them to the PYTHONPATH.
    // The resolving part is redundant in the case of --py-files, but necessary if the user
    // explicitly sets `spark.submit.pyFiles` in his/her default properties file.
    val pyFiles = sparkConf.get(SUBMIT_PYTHON_FILES)
    val resolvedPyFiles = Utils.resolveURIs(pyFiles.mkString(","))
    val formattedPyFiles = if (deployMode != CLUSTER) {
      PythonRunner.formatPaths(resolvedPyFiles).mkString(",")
    } else {
      // Ignoring formatting python path in yarn and mesos cluster mode, these two modes
      // support dealing with remote python files, they could distribute and add python files
      // locally.
      resolvedPyFiles
    }
    sparkConf.set(SUBMIT_PYTHON_FILES, formattedPyFiles.split(",").toSeq)

    (childArgs.toSeq, childClasspath.toSeq, sparkConf, childMainClass)
  }
```

该方法返回的是4元组：

- childArgs: Seq[String]

- childClasspath: Seq[String]

- sparkConf: org.apache.spark.SparkConf

- childMainClass: String

  在client模式下是`$args.mainClass`，yarn cluster模式下是`org.apache.spark.deploy.yarn.YarnClusterApplication`



## 2. 启动应用程序

如果是client模式，启动的是`JavaMainApplication`；但是如果是yarn cluster模式，它创建的实例是不同的，启动的类其实是`YarnClusterApplication`，同样继承了`SparkApplication`。

```scala
  // 通过classOf[]构建从属于mainClass的SparkApplication对象,然后通过mainclass实例化了SparkApplication
  val app: SparkApplication = if (classOf[SparkApplication].isAssignableFrom(mainClass)) {
      mainClass.getConstructor().newInstance().asInstanceOf[SparkApplication]
    } else {
      // 如果mainclass无法实例化SparkApplication,则使用替代构建子类JavaMainApplication实例
      new JavaMainApplication(mainClass)
    }
	
	// 
    @tailrec
    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException => if (e.getCause() != null) findCause(e.getCause()) else e
      case e: InvocationTargetException => if (e.getCause() != null) findCause(e.getCause()) else e
      case e: Throwable => e
    }

    try {
      // 启动实例
      app.start(childArgs.toArray, sparkConf)
    } catch {
      case t: Throwable => throw findCause(t)
    } finally {
      if (!isShell(args.primaryResource) && !isSqlShell(args.mainClass) && !isThriftServer(args.mainClass)) {
        try {
          	SparkContext.getActive.foreach(_.stop())
        } catch {
          	case e: Throwable => logError(s"Failed to close SparkContext: $e")
        }
      }
    }
```

## 3. JavaMainApplication

```scala
/**
 * Implementation of SparkApplication that wraps a standard Java class with a "main" method.
 *
 * Configuration is propagated to the application via system properties, so running multiple
 * of these in the same JVM may lead to undefined behavior due to configuration leaks.
 *
 * 用main方法包装标准java类的SparkApplication实现。配置是通过系统属性传递的,所以在同一个JVM中加载多个该类会可能导致配置泄露
 */
private[deploy] class JavaMainApplication(klass: Class[_]) extends SparkApplication {

    override def start(args: Array[String], conf: SparkConf): Unit = {
        // 通过反射获取静态main()方法
        val mainMethod = klass.getMethod("main", new Array[String](0).getClass)
        if (!Modifier.isStatic(mainMethod.getModifiers)) {
            throw new IllegalStateException("The main method in the given main class must be static")
        }

        // 获取配置，把配置写入sys
        val sysProps = conf.getAll.toMap
        sysProps.foreach { case (k, v) => sys.props(k) = v }

        // 调用真正主类（--class的类)的main()
        mainMethod.invoke(null, args)
    }
}

/**
 * Entry point for a Spark application. Implementations must provide a no-argument constructor.
 */
private[spark] trait SparkApplication {
    def start(args: Array[String], conf: SparkConf): Unit
}
```

