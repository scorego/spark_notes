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

最终执行的命令是：

```shell
/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.282.b08-1.el7_9.x86_64/bin/java \
	-cp /home/work/spark-3.1.1-bin-hadoop2.7/conf/:/home/work/spark-3.1.1-bin-hadoop2.7/jars/*
	org.apache.spark.deploy.SparkSubmit \
	--master yarn \
	--deploy-mode cluster \
	--class com.example.WordCount \
	--executor-memory 4G \
	./user-jar-0.1.1.jar \
	arg1 arg2
```

# 二、 `org.apache.spark.deploy.SparkSubmit`

从命令看到，`org.apache.spark.deploy.SparkSubmit`是作业的执行入口。具体到本条提交作业命令，该类主要做了以下事情：

1. 解析参数

   | `SparkSubmitOptionParser`的变量  | Conf key                    | 值                    |
   | -------------------------------- | --------------------------- | --------------------- |
   | `master: String`                 | `--master`                  | yarn                  |
   | `mainClass: String`              | `--class`                   | com.example.WordCount |
   | `deployMode: String`             | `--deploy-mode`             | cluster               |
   | `executorMemory: String`         | `--executor-memory`         | 4G                    |
   | `primaryResource: String`        | 第一个不认识的参数          | ./user-jar-0.1.1.jar  |
   | `childArgs: ArrayBuffer[String]` | `primaryResource`之后的参数 | [arg1, arg2]          |

   除了命令显示生命的参数，还会从`spark-defaults.conf`文件和环境变量中补充没有解析到的key。此外，`action`变量会缺省设为`SUBMIT`，除非有`--kill`等参数。

2. 根据`action`执行不同的动作，`SUBMIT`会执行`submit()`方法，该方法最终执行的是`runMain()`方法，主要有以下动作：

   - 解析参数，并返回`childArgs: ArrayBuffer[String]` / `childClasspath: ArrayBuffer[String]` / `sparkConf: SparkConf` / `childMainClass: String`。其中，`childMainClass: String`在不同部署模式下值如下：

     | 部署模式                  | 值                                                           |
     | ------------------------- | ------------------------------------------------------------ |
     | if deployMode == "client" | args.mainClass                                               |
     | Standalone Cluster        | `classOf[RestSubmissionClientApp].getName()`或`classOf[ClientApp].getName()` |
     | Mesos Cluster             | `classOf[RestSubmissionClientApp].getName()`                 |
     | YARN CLUSTER              | "org.apache.spark.deploy.yarn.YarnClusterApplication"        |
     | Kubernetes Cluster        | "org.apache.spark.deploy.k8s.submit.KubernetesClientApplication" |

   - 新建主类(`childMainClass`)的实例并调用其`start()`方法。

# 三、`org.apache.spark.deploy.yarn.YarnClusterApplication`

YARN CLUSTER模式西，新建的`childMainClass`是`YarnClusterApplication`。`YarnClusterApplication#start`方法其实是新建客户端并运行，代码如下：

```scala
private[spark] class YarnClusterApplication extends SparkApplication {
  override def start(args: Array[String], conf: SparkConf): Unit = {
    // SparkSubmit would use yarn cache to distribute files & jars in yarn mode,
    // so remove them from sparkConf here for yarn mode.
    conf.remove(JARS)
    conf.remove(FILES)
    conf.remove(ARCHIVES)
		
    // 初始化org.apache.spark.deploy.yarn.Client实例并调用其run()方法
    new Client(new ClientArguments(args), conf, null).run()
  }
}
```

# 四、 `org.apache.spark.deploy.yarn.Client`

`Client`类是客户端，负责提交脚本所在机器与YARN集群的通信。`Client`类会将一些jar包上传到hdfs并向RM申请资源以启动`ApplicationMaster`。

## 1. 初始化

```scala
package org.apache.spark.deploy.yarn

private[spark] class Client(
    val args: ClientArguments,
    val sparkConf: SparkConf,
    val rpcEnv: RpcEnv) extends Logging {
  
  // 创建一个YarnClient实例
  private val yarnClient = YarnClient.createYarnClient
  private val hadoopConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(sparkConf))

  private val isClusterMode = sparkConf.get(SUBMIT_DEPLOY_MODE) == "cluster"

  private val isClientUnmanagedAMEnabled = sparkConf.get(YARN_UNMANAGED_AM) && !isClusterMode
  private var appMaster: ApplicationMaster = _
  private var stagingDirPath: Path = _
  
  // AM相关的配置，主要是一些memort、cores的配置
  private val amMemory = if (isClusterMode) {
    sparkConf.get(DRIVER_MEMORY).toInt
  } else {
    sparkConf.get(AM_MEMORY).toInt
  }
  private val amMemoryOverhead = {
    val amMemoryOverheadEntry = if (isClusterMode) DRIVER_MEMORY_OVERHEAD else AM_MEMORY_OVERHEAD
    sparkConf.get(amMemoryOverheadEntry).getOrElse(
      math.max((MEMORY_OVERHEAD_FACTOR * amMemory).toLong, ResourceProfile.MEMORY_OVERHEAD_MIN_MIB)).toInt
  }
  private val amCores = if (isClusterMode) { sparkConf.get(DRIVER_CORES)} else { sparkConf.get(AM_CORES) }

  // Executor相关的配置
  private val executorMemory = sparkConf.get(EXECUTOR_MEMORY)
  protected val executorOffHeapMemory = Utils.executorOffHeapMemorySizeAsMb(sparkConf)
  private val executorMemoryOverhead = sparkConf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toLong, ResourceProfile.MEMORY_OVERHEAD_MIN_MIB)).toInt

  
  private val isPython = sparkConf.get(IS_PYTHON_APP)
  private val pysparkWorkerMemory: Int = if (isPython) {
    sparkConf.get(PYSPARK_EXECUTOR_MEMORY).map(_.toInt).getOrElse(0) } else { 0 }

  private val distCacheMgr = new ClientDistributedCacheManager()
  private val cachedResourcesConf = new SparkConf(false)

  private val keytab = sparkConf.get(KEYTAB).orNull
  private val amKeytabFileName: Option[String] = if (keytab != null && isClusterMode) {
    val principal = sparkConf.get(PRINCIPAL).orNull
    require((principal == null) == (keytab == null), "Both principal and keytab must be defined, or neither.")
    logInfo(s"Kerberos credentials: principal = $principal, keytab = $keytab")
    Some(new File(keytab).getName() + "-" + UUID.randomUUID().toString)
  } else {
    None
  }

  require(keytab == null || !Utils.isLocalUri(keytab), "Keytab should reference a local file.")

  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sparkConf

    override def onStopRequest(): Unit = {
      if (isClusterMode && appId != null) {
        yarnClient.killApplication(appId)
      } else {
        setState(SparkAppHandle.State.KILLED)
        stop()
      }
    }
  }
  private val fireAndForget = isClusterMode && !sparkConf.get(WAIT_FOR_APP_COMPLETION)

  private var appId: ApplicationId = null
}
```

## 2. `run()`方法

```scala
  /**
     * Submit an application to the ResourceManager.
     *
     * If set spark.yarn.submit.waitAppCompletion to true, it will stay alive
     * reporting the application's status until the application has exited for any reason.
     * Otherwise, the client process will exit after submission.
     *
     * If the application finishes with a failed, killed, or undefined status,
     * throw an appropriate SparkException.
     */
  def run(): Unit = {
    // 1. 提交应用程序，并返回appId
    this.appId = submitApplication()
    
    // 如果是cluster模式且配置了“spark.yarn.submit.waitAppCompletion”，则不断监控程序状态
    if (!launcherBackend.isConnected() && fireAndForget) {
      val report = getApplicationReport(appId)
      val state = report.getYarnApplicationState
      logInfo(s"Application report for $appId (state: $state)")
      logInfo(formatReportDetails(report, getDriverLogsLink(report)))
      if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
        throw new SparkException(s"Application $appId finished with status: $state")
      }
    } else {
      val YarnAppReport(appState, finalState, diags) = monitorApplication(appId)
      if (appState == YarnApplicationState.FAILED || finalState == FinalApplicationStatus.FAILED) {
        diags.foreach { err =>
          logError(s"Application diagnostics message: $err")
        }
        throw new SparkException(s"Application $appId finished with failed status")
      }
      if (appState == YarnApplicationState.KILLED || finalState == FinalApplicationStatus.KILLED) {
        throw new SparkException(s"Application $appId is killed")
      }
      if (finalState == FinalApplicationStatus.UNDEFINED) {
        throw new SparkException(s"The final status of application $appId is undefined")
      }
    }
  }
```

可以看到，`run()`方法的核心就是通过`submitApplication()`方法提交应用程序

## 3. `submitApplication()`方法

代码：

```scala
 /**
   * Submit an application running our ApplicationMaster to the ResourceManager.
   *
   * The stable Yarn API provides a convenience method (YarnClient#createApplication) for
   * creating applications and setting up the application submission context. This was not
   * available in the alpha API.
   */
  def submitApplication(): ApplicationId = {
    ResourceRequestHelper.validateResources(sparkConf)

    var appId: ApplicationId = null
    try {
      launcherBackend.connect()
      yarnClient.init(hadoopConf)
      yarnClient.start()

      logInfo("Requesting a new application from cluster with %d NodeManagers"
        .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

      // 1. Get a new application from our RM
      val newApp = yarnClient.createApplication()
      val newAppResponse = newApp.getNewApplicationResponse()
      appId = newAppResponse.getApplicationId()

      // 2. 创建staging dir
      // scalastyle:off FileSystemGet
      val appStagingBaseDir = sparkConf.get(STAGING_DIR)
        .map { new Path(_, UserGroupInformation.getCurrentUser.getShortUserName) }
        .getOrElse(FileSystem.get(hadoopConf).getHomeDirectory())
      stagingDirPath = new Path(appStagingBaseDir, getAppStagingDir(appId))
      // scalastyle:on FileSystemGet

      new CallerContext("CLIENT", sparkConf.get(APP_CALLER_CONTEXT),
        Option(appId.toString)).setCurrentContext()

      // 3. Verify whether the cluster has enough resources for our AM
      verifyClusterResources(newAppResponse)

      // 4. Set up the appropriate contexts to launch our AM
      val containerContext = createContainerLaunchContext(newAppResponse)
      val appContext = createApplicationSubmissionContext(newApp, containerContext)

      // 5. submit and monitor the application，这就是向yarn提交应用
      logInfo(s"Submitting application $appId to ResourceManager")
      yarnClient.submitApplication(appContext)
      launcherBackend.setAppId(appId.toString)
      reportLauncherState(SparkAppHandle.State.SUBMITTED)

      appId
    } catch {
      case e: Throwable =>
        if (stagingDirPath != null) { cleanupStagingDir() }
        throw e
    }
  }
```

里面比较重要的是第4步，设置启动AM的contexts：

```scala
val containerContext = createContainerLaunchContext(newAppResponse)
val appContext = createApplicationSubmissionContext(newApp, containerContext)
```

### a. `createContainerLaunchContext()`

本方法返回类型是`ContainerLaunchContext`，`ContainerLaunchContext`包含了`NodeManager`启动一个container所需要的信息，主要就是启动命令。

```scala
/**
  * Set up a ContainerLaunchContext to launch our ApplicationMaster container.
  * This sets up the launch environment, java options, and the command for launching the AM.
  */
private def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse): ContainerLaunchContext = {
  logInfo("Setting up container launch context for our AM")
  val appId = newAppResponse.getApplicationId
  val pySparkArchives = if (sparkConf.get(IS_PYTHON_APP)) { findPySparkArchives() } else { Nil }

  val launchEnv = setupLaunchEnv(stagingDirPath, pySparkArchives)
  val localResources = prepareLocalResources(stagingDirPath, pySparkArchives)

  val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
  amContainer.setLocalResources(localResources.asJava)
  amContainer.setEnvironment(launchEnv.asJava)

  val javaOpts = ListBuffer[String]()

  // Set the environment variable through a command prefix
  // to append to the existing value of the variable
  var prefixEnv: Option[String] = None

  // Add Xmx for AM memory
  javaOpts += "-Xmx" + amMemory + "m"

  val tmpDir = new Path(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)
  javaOpts += "-Djava.io.tmpdir=" + tmpDir

  // TODO: Remove once cpuset version is pushed out.
  // The context is, default gc for server class machines ends up using all cores to do gc -
  // hence if there are multiple containers in same node, Spark GC affects all other containers'
  // performance (which can be that of other Spark containers)
  // Instead of using this, rely on cpusets by YARN to enforce "proper" Spark behavior in
  // multi-tenant environments. Not sure how default Java GC behaves if it is limited to subset
  // of cores on a node.
  val useConcurrentAndIncrementalGC = launchEnv.get("SPARK_USE_CONC_INCR_GC").exists(_.toBoolean)
  if (useConcurrentAndIncrementalGC) {
      // In our expts, using (default) throughput collector has severe perf ramifications in
      // multi-tenant machines
      javaOpts += "-XX:+UseConcMarkSweepGC"
      javaOpts += "-XX:MaxTenuringThreshold=31"
      javaOpts += "-XX:SurvivorRatio=8"
      javaOpts += "-XX:+CMSIncrementalMode"
      javaOpts += "-XX:+CMSIncrementalPacing"
      javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
      javaOpts += "-XX:CMSIncrementalDutyCycle=10"
  }

  // Include driver-specific java options if we are launching a driver
  if (isClusterMode) {
    sparkConf.get(DRIVER_JAVA_OPTIONS).foreach { opts =>
      javaOpts ++= Utils.splitCommandString(opts)
      .map(Utils.substituteAppId(_, appId.toString))
      .map(YarnSparkHadoopUtil.escapeForShell)
    }
    val libraryPaths = Seq(sparkConf.get(DRIVER_LIBRARY_PATH),
                           sys.props.get("spark.driver.libraryPath")).flatten
    if (libraryPaths.nonEmpty) {
      prefixEnv = Some(createLibraryPathPrefix(libraryPaths.mkString(File.pathSeparator),
                                               sparkConf))
    }
    if (sparkConf.get(AM_JAVA_OPTIONS).isDefined) {
      logWarning(s"${AM_JAVA_OPTIONS.key} will not take effect in cluster mode")
    }
  } else {
    // Validate and include yarn am specific java options in yarn-client mode.
    sparkConf.get(AM_JAVA_OPTIONS).foreach { opts =>
      if (opts.contains("-Dspark")) {
        val msg = s"${AM_JAVA_OPTIONS.key} is not allowed to set Spark options (was '$opts')."
        throw new SparkException(msg)
      }
      if (opts.contains("-Xmx")) {
        val msg = s"${AM_JAVA_OPTIONS.key} is not allowed to specify max heap memory settings " +
        s"(was '$opts'). Use spark.yarn.am.memory instead."
        throw new SparkException(msg)
      }
      javaOpts ++= Utils.splitCommandString(opts)
      .map(Utils.substituteAppId(_, appId.toString))
      .map(YarnSparkHadoopUtil.escapeForShell)
    }
    sparkConf.get(AM_LIBRARY_PATH).foreach { paths =>
      prefixEnv = Some(createLibraryPathPrefix(paths, sparkConf))
    }
  }

  javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)
	
  // 用户的主类
  val userClass =
    if (isClusterMode) {
      Seq("--class", YarnSparkHadoopUtil.escapeForShell(args.userClass))
    } else {
      Nil
    }
  val userJar =
    if (args.userJar != null) {
      Seq("--jar", args.userJar)
    } else {
      Nil
    }
  val primaryPyFile =
    if (isClusterMode && args.primaryPyFile != null) {
      Seq("--primary-py-file", new Path(args.primaryPyFile).getName())
    } else {
      Nil
    }
  val primaryRFile =
    if (args.primaryRFile != null) {
      Seq("--primary-r-file", args.primaryRFile)
    } else {
      Nil
    }
  
  // 这里是ApplicationMaster的类名
  val amClass =
    if (isClusterMode) {
      Utils.classForName("org.apache.spark.deploy.yarn.ApplicationMaster").getName
    } else {
      Utils.classForName("org.apache.spark.deploy.yarn.ExecutorLauncher").getName
    }
  if (args.primaryRFile != null &&
      (args.primaryRFile.endsWith(".R") || args.primaryRFile.endsWith(".r"))) {
    args.userArgs = ArrayBuffer(args.primaryRFile) ++ args.userArgs
  }
  val userArgs = args.userArgs.flatMap { arg => Seq("--arg", YarnSparkHadoopUtil.escapeForShell(arg)) }
  val amArgs =
    Seq(amClass) ++ userClass ++ userJar ++ primaryPyFile ++ primaryRFile ++ userArgs ++
    Seq("--properties-file",
        buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, SPARK_CONF_FILE)) ++
    Seq("--dist-cache-conf",
        buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, DIST_CACHE_CONF_FILE))

  // Command for the ApplicationMaster
  val commands = prefixEnv ++
    Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server") ++
    javaOpts ++ amArgs ++
    Seq(
      "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
      "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

  val printableCommands = commands.map(s => if (s == null) "null" else s).toList
  amContainer.setCommands(printableCommands.asJava)

  logDebug("===============================================================================")
  logDebug("YARN AM launch context:")
  logDebug(s"    user class: ${Option(args.userClass).getOrElse("N/A")}")
  logDebug("    env:")
  if (log.isDebugEnabled) {
    Utils.redact(sparkConf, launchEnv.toSeq).foreach { case (k, v) => logDebug(s"        $k -> $v")}
  }
  logDebug("    resources:")
  localResources.foreach { case (k, v) => logDebug(s"        $k -> $v")}
  logDebug("    command:")
  logDebug(s"        ${printableCommands.mkString(" ")}")
  logDebug("===============================================================================")

  // send the acl settings into YARN to control who has access via YARN interfaces
  val securityManager = new SecurityManager(sparkConf)
  amContainer.setApplicationACLs(YarnSparkHadoopUtil.getApplicationAclsForYarn(securityManager).asJava)
  setupSecurityToken(amContainer)
  amContainer
}
```

可以看到，Cluster模式下，AppMaster的类是`org.apache.spark.deploy.yarn.ApplicationMaster`，Client模式下类是`org.apache.spark.deploy.yarn.ExecutorLauncher`。

### b. `createApplicationSubmissionContext()`

参数包括`yarnClient`创建的`YarnClientApplication`和上面的`ContainerLaunchContext`，返回的是`org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext`类。

# 五、`org.apache.spark.deploy.yarn.ApplicationMaster`

`ApplicationMaster`的静态`main()`方法的核心逻辑是新建一个`ApplicationMaster`实例并调用该实例的`run()`方法：

```scala
final def run(): Int = {
  try {
    val attemptID = if (isClusterMode) {
      if (System.getProperty(UI_PORT.key) == null) { System.setProperty(UI_PORT.key, "0") }
      System.setProperty("spark.master", "yarn")
      System.setProperty(SUBMIT_DEPLOY_MODE.key, "cluster")
      System.setProperty("spark.yarn.app.id", appAttemptId.getApplicationId().toString())

      Option(appAttemptId.getAttemptId.toString)
    } else {
      None
    }

    new CallerContext("APPMASTER", 
                      sparkConf.get(APP_CALLER_CONTEXT),
                      Option(appAttemptId.getApplicationId.toString), 
                      attemptID).setCurrentContext()

    logInfo("ApplicationAttemptId: " + appAttemptId)

    val priority = ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY - 1
    ShutdownHookManager.addShutdownHook(priority) { () =>
      val maxAppAttempts = client.getMaxRegAttempts(sparkConf, yarnConf)
      val isLastAttempt = appAttemptId.getAttemptId() >= maxAppAttempts

      if (!finished) {
        finish(finalStatus,
               ApplicationMaster.EXIT_EARLY,
               "Shutdown hook called before final status was reported.")
      }

      if (!unregistered) {
        if (finalStatus == FinalApplicationStatus.SUCCEEDED || isLastAttempt) {
          unregister(finalStatus, finalMsg)
          cleanupStagingDir(new Path(System.getenv("SPARK_YARN_STAGING_DIR")))
        }
      }
    }
		
    // 如果是cluster模式，就runDriver()；否则就runExecutorLauncher()
    if (isClusterMode) {
      runDriver()
    } else {
      runExecutorLauncher()
    }
  } catch {
    case e: Exception =>
        // catch everything else if not specifically handled
        logError("Uncaught exception: ", e)
        finish(FinalApplicationStatus.FAILED,
               ApplicationMaster.EXIT_UNCAUGHT_EXCEPTION,
               "Uncaught exception: " + StringUtils.stringifyException(e))
  } finally {
    try {
      metricsSystem.foreach { ms =>
        ms.report()
        ms.stop()
      }
    } catch {
      case e: Exception => logWarning("Exception during stopping of the metric system: ", e)
    }
  }
  exitCode
}
```

cluster模式会执行`runDriver()`方法：

```scala
private def runDriver(): Unit = {
  addAmIpFilter(None, System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV))
  
  // 在一个独立线程执行用户类，这就是用户的脚本，包含创建SparkContext的代码及具体业务逻辑
  userClassThread = startUserApplication()

  logInfo("Waiting for spark context initialization...")
  val totalWaitTime = sparkConf.get(AM_MAX_WAIT_TIME)
  try {
    val sc = ThreadUtils.awaitResult(sparkContextPromise.future,
                                     Duration(totalWaitTime, TimeUnit.MILLISECONDS))
    if (sc != null) {
      val rpcEnv = sc.env.rpcEnv

      val userConf = sc.getConf
      val host = userConf.get(DRIVER_HOST_ADDRESS)
      val port = userConf.get(DRIVER_PORT)
      // registerAM
      registerAM(host, port, userConf, sc.ui.map(_.webUrl), appAttemptId)

      val driverRef = rpcEnv.setupEndpointRef(RpcAddress(host, port), YarnSchedulerBackend.ENDPOINT_NAME)
      // createAllocator
      createAllocator(driverRef, userConf, rpcEnv, appAttemptId, distCacheConf)
    } else {
      throw new IllegalStateException("User did not initialize spark context!")
    }
    resumeDriver()
    userClassThread.join()
  } catch {
    case e: SparkException if e.getCause().isInstanceOf[TimeoutException] =>
        logError(s"SparkContext did not initialize after waiting for $totalWaitTime ms. " +
          				"Please check earlier log output for errors. Failing the application.")
        finish(FinalApplicationStatus.FAILED, ApplicationMaster.EXIT_SC_NOT_INITED,
               "Timed out waiting for SparkContext.")
  } finally {
    resumeDriver()
  }
}
```

