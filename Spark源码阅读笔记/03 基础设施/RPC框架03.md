# 一、 `TransportContext`

`TransportContext`内部包含配置信息`TransportConf`和对客户端请求消息进行处理的`RpcHandler`。

```java
package org.apache.spark.network;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */
public class TransportContext implements Closeable {
    
    // 配置信息
    private final TransportConf conf;
    
    // 对客户端请求消息进行处理的RPCHandler
    private final RpcHandler rpcHandler;

    private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
    private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;
	
    // 创建RPC客户端工厂类
    public TransportClientFactory createClientFactory() { return createClientFactory(new ArrayList<>()); }
    public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
        return new TransportClientFactory(this, bootstraps);
    }
    
    // 创建RPC服务端
    public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, null, port, rpcHandler, bootstraps);
    }
    public TransportServer createServer(String host, int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, host, port, rpcHandler, bootstraps);
    }
    public TransportServer createServer(List<TransportServerBootstrap> bootstraps) { return createServer(0, bootstraps);}
    public TransportServer createServer() { return createServer(0, new ArrayList<>()); }
	
    ...
}
```

# 二、`TransportConf`

```java
package org.apache.spark.network.util;

public class TransportConf {

    private final ConfigProvider conf;

    private final String module;

    private String getConfKey(String suffix) {
        return "spark." + module + "." + suffix;
    }

    public TransportConf(String module, ConfigProvider conf) {
        this.module = module;
        this.conf = conf;
        SPARK_NETWORK_IO_MODE_KEY = getConfKey("io.mode");
        SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY = getConfKey("io.preferDirectBufs");
        SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY = getConfKey("io.connectionTimeout");
        SPARK_NETWORK_IO_BACKLOG_KEY = getConfKey("io.backLog");
        SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY =  getConfKey("io.numConnectionsPerPeer");
        SPARK_NETWORK_IO_SERVERTHREADS_KEY = getConfKey("io.serverThreads");
        SPARK_NETWORK_IO_CLIENTTHREADS_KEY = getConfKey("io.clientThreads");
        SPARK_NETWORK_IO_RECEIVEBUFFER_KEY = getConfKey("io.receiveBuffer");
        SPARK_NETWORK_IO_SENDBUFFER_KEY = getConfKey("io.sendBuffer");
        SPARK_NETWORK_SASL_TIMEOUT_KEY = getConfKey("sasl.timeout");
        SPARK_NETWORK_IO_MAXRETRIES_KEY = getConfKey("io.maxRetries");
        SPARK_NETWORK_IO_RETRYWAIT_KEY = getConfKey("io.retryWait");
        SPARK_NETWORK_IO_LAZYFD_KEY = getConfKey("io.lazyFD");
        SPARK_NETWORK_VERBOSE_METRICS = getConfKey("io.enableVerboseMetrics");
        SPARK_NETWORK_IO_ENABLETCPKEEPALIVE_KEY = getConfKey("io.enableTcpKeepAlive");
    }
    
    ...
}
```

Spark通常使用`SparkTransportConf`创建`TransportConf`，代码如下：

```scala
package org.apache.spark.network.netty

// 将SparkConf(携带环境信息，如JVM的core数)转换成SparkTransportConf
object SparkTransportConf {

    def fromSparkConf(_conf: SparkConf,
                      module: String,
                      numUsableCores: Int = 0,
                      role: Option[String] = None): TransportConf = {
        val conf = _conf.
        val numThreads = NettyUtils.defaultNumThreads(numUsableCores)
        // override threads configurations with role specific values if specified
        // config order is role > module > default
        Seq("serverThreads", "clientThreads").foreach { suffix =>
            val value = role.flatMap { r => conf.getOption(s"spark.$r.$module.io.$suffix") }
            	.getOrElse(conf.get(s"spark.$module.io.$suffix", numThreads.toString))
            
            conf.set(s"spark.$module.io.$suffix", value)
        }
		
        // ConfigProvider的get方法实际上是代理了SparkConf的get方法
        new TransportConf(module, new ConfigProvider {
            override def get(name: String): String = conf.get(name)
            override def get(name: String, defaultValue: String): String = conf.get(name, defaultValue)
            override def getAll(): java.lang.Iterable[java.util.Map.Entry[String, String]] = {
                conf.getAll.toMap.asJava.entrySet()
            }
        })
    }
}
```

# 三、`TransportClientFactory`

`TransportClientFactory`是创建`TransportClient`的工厂类。`TransportContext`在创建`TransportClientFactory`的时候会传递两个参数：`TransportContext`和`TransportClientBootstrap`。

```java
public TransportClientFactory createClientFactory() { return createClientFactory(new ArrayList<>()); }
public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
	return new TransportClientFactory(this, bootstraps);
}
```

`TransportClientFactory`代码如下：

```java
package org.apache.spark.network.client;


/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 */
public class TransportClientFactory implements Closeable {

    private final TransportContext context;
    private final TransportConf conf;
    private final List<TransportClientBootstrap> clientBootstraps;
    // 针对每个Socket地址的连接池ClientPool的缓存
    private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;   
	
    // 对Socket连接池ClientPool中缓存的TransportClient进行随机选择，对每个链接进行负载均衡
    private final Random rand;
    private final int numConnectionsPerPeer;
	
    // 客户端Channel被创建使用的类，通过ioMpde匹配，NioSocketChannel.class或EpollSocketChannel.class
    private final Class<? extends Channel> socketChannelClass;
    // NioEventLoopGroup或EpollEventLoopGroup
    private EventLoopGroup workerGroup;
    private final PooledByteBufAllocator pooledAllocator;
    private final NettyMemoryMetrics metrics;
    private final int fastFailTimeWindow;

    public TransportClientFactory(TransportContext context,
                                  List<TransportClientBootstrap> clientBootstraps) {
        this.context = Preconditions.checkNotNull(context);
        this.conf = context.getConf();
        this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
        this.connectionPool = new ConcurrentHashMap<>();
        this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
        this.rand = new Random();
		
        // IO模式，NIO或者EPOLL
        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
        this.workerGroup = NettyUtils.createEventLoop(
            ioMode,
            conf.clientThreads(),
            conf.getModuleName() + "-client");
        if (conf.sharedByteBufAllocators()) {
            this.pooledAllocator = NettyUtils.getSharedPooledByteBufAllocator(
                conf.preferDirectBufsForSharedByteBufAllocators(), false /* allowCache */);
        } else {
            this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
                conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
        }
        this.metrics = new NettyMemoryMetrics(
            this.pooledAllocator, conf.getModuleName() + "-client", conf);
        fastFailTimeWindow = (int)(conf.ioRetryWaitTimeMs() * 0.95);
    }

}
```

## 1. 客户端引导程序`TransportClientBootstrap`

`TransportClientBootstrap`是在`TransportClient`上执行的客户端引导程序，主要是连接建立时进行一些初始化的准备（如验证、加密）。这些操作是昂贵的，不过建立的连接可以重用。

```java
package org.apache.spark.network.client;

import io.netty.channel.Channel;

/**
 * A bootstrap which is executed on a TransportClient before it is returned to the user.
 * This enables an initial exchange of information (e.g., SASL authentication tokens) on a once-per-
 * connection basis.
 *
 * Since connections (and TransportClients) are reused as much as possible, it is generally
 * reasonable to perform an expensive bootstrapping operation, as they often share a lifespan with
 * the JVM itself.
 */
public interface TransportClientBootstrap {
  /** Performs the bootstrapping operation, throwing an exception on failure. */
  void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}	
```

一共有两个实现类：

```java
// performing authentication using Spark's auth protocol
package org.apache.spark.network.crypto;
public class AuthClientBootstrap implements TransportClientBootstrap
    
// performing SASL authentication on the connection
package org.apache.spark.network.sasl;
public class SaslClientBootstrap implements TransportClientBootstrap 
```

## 2. RPC客户端`TransportClient`

`TransportClient`是RPC客户端，每个客户端实例只能和一个远端的RPC服务通信，如果一个组件想要和多个RPC服务通信，就需要持有多个客户端实例。`TransportClientFactory`创建客户端的代码如下：

```java

```



