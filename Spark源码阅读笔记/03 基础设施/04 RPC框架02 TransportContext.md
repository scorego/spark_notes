`TransportContext`内部包含传输配置信息`TransportConf`和对收到的RPC消息进行处理的`RpcHandler`，可以用来创建RPC客户端工厂类 `TransportClientFactory`和RPC服务端`TransportServer`。`TransportContext`内部握有创建`TransPortClient`和`TransPortServer`的方法实现，但却属于最底层的RPC通讯设施，这是因为其成员变量`RPCHandler`是抽象的，并没有具体的消息处理。高层封装为`NettyRPCEnv`，并持有`TransportContext`引用，在`TransportContext`中传入`NettyRpcHandler`实体，来实现Netty通讯回调Handler处理。

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

# 一、`TransportConf`

`TransportConf`是传输配置信息。

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

Spark通常使用`SparkTransportConf.SparkTransportConf()`方法创建`TransportConf`，代码如下：

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

# 二、RPC客户端工厂类 `TransportClientFactory`

`TransportClientFactory`是创建`TransportClient`的工厂类。`TransportContext`在创建`TransportClientFactory`的时候会传递两个参数：`TransportContext`和`TransportClientBootstrap`。

```java
public TransportClientFactory createClientFactory() { 
  return createClientFactory(new ArrayList<>()); 
}

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
        this.workerGroup = NettyUtils.createEventLoop(ioMode, 
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
	
    ...
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

## 2. `createClient()`

`TransportClient`是RPC客户端，每个客户端实例只能和一个远端的RPC服务通信，如果一个组件想要和多个RPC服务通信，就需要持有多个客户端实例。`TransportClientFactory`创建客户端的代码如下：

```java
public TransportClient createClient(String remoteHost, int remotePort) 
  throws IOException, InterruptedException {
    return createClient(remoteHost, remotePort, false);
}

public TransportClient createClient(String remoteHost, int remotePort, boolean fastFail)
  throws IOException, InterruptedException {
    //  先构建InetSocketAddress
    final InetSocketAddress unresolvedAddress = InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool = connectionPool.get(unresolvedAddress);
    if (clientPool == null) {
        connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
        clientPool = connectionPool.get(unresolvedAddress);
    }

    // 随机产生一个索引以供选择TransportClient
    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
        // Make sure that the channel will not timeout by updating the last use time of the
        // handler. Then check that the client is still alive, in case it timed out before
        // this code was able to update things.
        TransportChannelHandler handler = 
          	cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
        synchronized (handler) {
            handler.getResponseHandler().updateTimeOfLastRequest();
        }

        if (cachedClient.isActive()) {
            logger.trace("Returning cached connection to {}: {}",
                         cachedClient.getSocketAddress(), cachedClient);
            return cachedClient;
        }
    }

    // 当索引位置没有TransportClient或者TransportClient没有激活，则构建InetSocketAddress（会域名解析）
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    final String resolvMsg = resolvedAddress.isUnresolved() ? "failed" : "succeed";
    if (hostResolveTimeMs > 2000) {
        logger.warn("DNS resolution {} for {} took {} ms", resolvMsg, resolvedAddress, hostResolveTimeMs);
    } else {
        logger.trace("DNS resolution {} for {} took {} ms", resolvMsg, resolvedAddress, hostResolveTimeMs);
    }
    // 调用createClient()方法创建并返回TransportClient(创建前双重检查)
    synchronized (clientPool.locks[clientIndex]) {
        cachedClient = clientPool.clients[clientIndex];

        if (cachedClient != null) {
            if (cachedClient.isActive()) {
                logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
                return cachedClient;
            } else {
                logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
            }
        }
        // If this connection should fast fail when last connection failed in last fast fail time
        // window and it did, fail this connection directly.
        if (fastFail && System.currentTimeMillis() - clientPool.lastConnectionFailed < fastFailTimeWindow) {
            throw new IOException(String.format("Connecting to %s failed in the last %s ms, fail this connection directly",
                                                resolvedAddress, 
                                                fastFailTimeWindow));
        }
        try {
            clientPool.clients[clientIndex] = createClient(resolvedAddress);
            clientPool.lastConnectionFailed = 0;
        } catch (IOException e) {
            clientPool.lastConnectionFailed = System.currentTimeMillis();
            throw e;
        }
        return clientPool.clients[clientIndex];
    }
}

// 创建TransportClient
TransportClient createClient(InetSocketAddress address) throws IOException, InterruptedException {
    
    logger.debug("Creating new connection to {}", address);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
        .channel(socketChannelClass)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
        .option(ChannelOption.ALLOCATOR, pooledAllocator);
    
    if (conf.receiveBuf() > 0) { bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf()); }
    if (conf.sendBuf() > 0) { bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf()); }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(SocketChannel ch) {
            TransportChannelHandler clientHandler = context.initializePipeline(ch);
            clientRef.set(clientHandler.getClient());
            channelRef.set(ch);
        }
    });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.await(conf.connectionTimeoutMs())) {
        throw new IOException(String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
        throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // 使用clientBootstraps
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    try {
        for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
            clientBootstrap.doBootstrap(client, channel);
        }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
        long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
        logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
        client.close();
        throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();

    logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
                address, 
                (postBootstrap - preConnect) / 1000000,
                (postBootstrap - preBootstrap) / 1000000);

    return client;
}
```

## 3. RPC客户端`TransportClient`

```java
package org.apache.spark.network.client;

public class TransportClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

    private final Channel channel;
    private final TransportResponseHandler handler;
    @Nullable private String clientId;
    private volatile boolean timedOut;

    // 从远端写上好的流中请求单个块
    public void fetchChunk(long streamId,
                           int chunkIndex,
                           ChunkReceivedCallback callback) {
        if (logger.isDebugEnabled()) {
            logger.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
        }

        StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
        StdChannelListener listener = new StdChannelListener(streamChunkId) {
            @Override
            void handleFailure(String errorMsg, Throwable cause) {
                handler.removeFetchRequest(streamChunkId);
                callback.onFailure(chunkIndex, new IOException(errorMsg, cause));
            }
        };
        handler.addFetchRequest(streamChunkId, callback);

        channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(listener);
    }

    public void send(ByteBuffer message) {
        channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
    }


    private static long requestId() { return Math.abs(UUID.randomUUID().getLeastSignificantBits()); }

    public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending RPC to {}", getRemoteAddress(channel));
        }
		// 生成UUID作为requestId，并向TransportResponseHandler添加requestId和回调类RpcResponseCallback的对应关系
        long requestId = requestId();
        handler.addRpcRequest(requestId, callback);
		
        // 调用io.netty.channel.Channel的writeAndFlush()方法将RPC请求发送出去
        RpcChannelListener listener = new RpcChannelListener(requestId, callback);
        channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message))).addListener(listener);

        return requestId;
    }

}
```



# 三、 RPC服务端`TransportServer`

`TransportServer`是RPC框架的服务端，`TransportContext`的`createServer()`方法用于创建`TransportServer`实例，最终调用的是`TransportServer`的构造器。

```java
public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
}

public TransportServer createServer(String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
}

public TransportServer createServer(List<TransportServerBootstrap> bootstraps) { 
  	return createServer(0, bootstraps); 
}

public TransportServer createServer() { 
  	return createServer(0, new ArrayList<>()); 
}
```

`TransportServer`的构造器如下：

```java
package org.apache.spark.network.server;

/**
  * Server for the efficient, low-level streaming service.
  */
public class TransportServer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportServer.class);

    private final TransportContext context;
    private final TransportConf conf;
    private final RpcHandler appRpcHandler;
    private final List<TransportServerBootstrap> bootstraps;

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private int port = -1;
    private final PooledByteBufAllocator pooledAllocator;
    private NettyMemoryMetrics metrics;

    /**
   * Creates a TransportServer that binds to the given host and the given port, or to any available
   * if 0. If you don't want to bind to any special host, set "hostToBind" to null.
   * */
    public TransportServer(TransportContext context,
                           String hostToBind,
                           int portToBind,
                           RpcHandler appRpcHandler,
                           List<TransportServerBootstrap> bootstraps) {
        this.context = context;
        this.conf = context.getConf();
        this.appRpcHandler = appRpcHandler;
        if (conf.sharedByteBufAllocators()) {
            this.pooledAllocator = NettyUtils.getSharedPooledByteBufAllocator(
                conf.preferDirectBufsForSharedByteBufAllocators(), true /* allowCache */);
        } else {
            this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
                conf.preferDirectBufs(), true /* allowCache */, conf.serverThreads());
        }
        this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));

        boolean shouldClose = true;
        try {
            init(hostToBind, portToBind);
            shouldClose = false;
        } finally {
            if (shouldClose) {
                JavaUtils.closeQuietly(this);
            }
        }
    }
    
    // 初始化
    private void init(String hostToBind, int portToBind) {
        // 根据Netty的API文档，Netty服务端需同时创建bossGroup和workerGroup
        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        EventLoopGroup bossGroup = NettyUtils.createEventLoop(ioMode, 1, conf.getModuleName() + "-boss");
        EventLoopGroup workerGroup =  NettyUtils.createEventLoop(ioMode, conf.serverThreads(),
                                                                 conf.getModuleName() + "-server");

        bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NettyUtils.getServerChannelClass(ioMode))
            .option(ChannelOption.ALLOCATOR, pooledAllocator)
            .option(ChannelOption.SO_REUSEADDR, !SystemUtils.IS_OS_WINDOWS)
            .childOption(ChannelOption.ALLOCATOR, pooledAllocator);

        this.metrics = new NettyMemoryMetrics(pooledAllocator, conf.getModuleName() + "-server", conf);

        if (conf.backLog() > 0) { bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog()); }
        if (conf.receiveBuf() > 0) { bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf()); }
        if (conf.sendBuf() > 0) { bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf()); }
        if (conf.enableTcpKeepAlive()) { bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true); }

        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                logger.debug("New connection accepted for remote address {}.", ch.remoteAddress());
                RpcHandler rpcHandler = appRpcHandler;
                for (TransportServerBootstrap bootstrap : bootstraps) {
                    rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
                }
                context.initializePipeline(ch, rpcHandler);
            }
        });
		
        // 给引导程序绑定Socket的监听端口
        InetSocketAddress address = hostToBind == null ?
            new InetSocketAddress(portToBind): new InetSocketAddress(hostToBind, portToBind);
        channelFuture = bootstrap.bind(address);
        channelFuture.syncUninterruptibly();

        port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        logger.debug("Shuffle server started on port: {}", port);
    }
}
```

## 1. 服务端引导程序`TransportServerBootstrap`

服务端引导程序`TransportServerBootstrap`旨在当客户端与服务端建立连接之后，在服务端持有的客户端管道上执行的引导程序。

```java
package org.apache.spark.network.server;

import io.netty.channel.Channel;

/**
 * A bootstrap which is executed on a TransportServer's client channel once a client connects
 * to the server. This allows customizing the client channel to allow for things such as SASL
 * authentication.
 */
public interface TransportServerBootstrap {

  RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler);
    
}
```

一共有两个实现类：

```java
package org.apache.spark.network.crypto;
public class AuthServerBootstrap implements TransportServerBootstrap
    
package org.apache.spark.network.sasl;
public class SaslServerBootstrap implements TransportServerBootstrap
```

# 四、 `initializePipeline()`

`TransportClientFactory`创建`TransportClient`和`TransportContext`创建`TransportServer`的初始化中，调用了`TransportContext`的`initializePipeline()`方法，`initializePipeline()`的用途是调用Netty的API对管道初始化，代码如下:

```java
/**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel 				The channel to initialize.
   * @param channelRpcHandler 		The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
public TransportChannelHandler initializePipeline(SocketChannel channel,
                                                  RpcHandler channelRpcHandler) {
    try {
        // createChannelHandler()方法用于创建TransportChannelHandler
        TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);

        // 对管道进行设置
        ChannelPipeline pipeline = channel.pipeline()
            .addLast("encoder", ENCODER)
            .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
            .addLast("decoder", DECODER)
            .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
            .addLast("handler", channelHandler);

        if (chunkFetchWorkers != null) {
            ChunkFetchRequestHandler chunkFetchHandler = new ChunkFetchRequestHandler(
                channelHandler.getClient(), 
                rpcHandler.getStreamManager(),
                conf.maxChunksBeingTransferred(), 
                true /* syncModeEnabled */);
            pipeline.addLast(chunkFetchWorkers, "chunkFetchHandler", chunkFetchHandler);
        }
        return channelHandler;
    } catch (RuntimeException e) {
        logger.error("Error while initializing Netty pipeline", e);
        throw e;
    }
}

/**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    // 其实这里才是真正创建TransportClient
    TransportClient client = new TransportClient(channel, responseHandler);
    boolean separateChunkFetchRequest = conf.separateChunkFetchRequest();
    ChunkFetchRequestHandler chunkFetchRequestHandler = null;
    if (!separateChunkFetchRequest) {
        chunkFetchRequestHandler = new ChunkFetchRequestHandler(client, 
                                                                rpcHandler.getStreamManager(), 
                                                                conf.maxChunksBeingTransferred(), 
                                                                false /* syncModeEnabled */);
    }
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, 
                                                                         client,
                                                                         rpcHandler, 
                                                                         conf.maxChunksBeingTransferred(),
                                                                         chunkFetchRequestHandler);
    return new TransportChannelHandler(client, 
                                       responseHandler, 
                                       requestHandler,
                                       conf.connectionTimeoutMs(), 
                                       separateChunkFetchRequest, 
                                       closeIdleConnections, 
                                       this);
}
```

## 1. `TransportChannelHandler`

`TransportChannelHandler`实现了Netty的`SimpleChannelInboundHandler`抽象类，以便对Netty管道中的消息进行处理。

```java
package org.apache.spark.network.server;

/**
 * The single Transport-level Channel handler which is used for delegating requests to the
 * {@link TransportRequestHandler} and responses to the {@link TransportResponseHandler}.
 *
 * All channels created in the transport layer are bidirectional. When the Client initiates a Netty
 * Channel with a RequestMessage (which gets handled by the Server's RequestHandler), the Server
 * will produce a ResponseMessage (handled by the Client's ResponseHandler). However, the Server
 * also gets a handle on the same Channel, so it may then begin to send RequestMessages to the
 * Client.
 * This means that the Client also needs a RequestHandler and the Server needs a ResponseHandler,
 * for the Client's responses to the Server's requests.
 *
 * This class also handles timeouts from a {@link io.netty.handler.timeout.IdleStateHandler}.
 * We consider a connection timed out if there are outstanding fetch or RPC requests but no traffic
 * on the channel for at least `requestTimeoutMs`. Note that this is duplex traffic; we will not
 * timeout if the client is continuously sending but getting no responses, for simplicity.
 */
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
    private static final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

    private final TransportClient client;
    private final TransportResponseHandler responseHandler;
    private final TransportRequestHandler requestHandler;
    private final long requestTimeoutNs;
    private final boolean closeIdleConnections;
    private final boolean skipChunkFetchRequest;
    private final TransportContext transportContext;

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Message request) throws Exception {
        if (request instanceof RequestMessage) {
            requestHandler.handle((RequestMessage) request);
        } else if (request instanceof ResponseMessage) {
            responseHandler.handle((ResponseMessage) request);
        } else {
            ctx.fireChannelRead(request);
        }
    }
 
    ...
}
```

可以从`channelRead0()`方法看到，当`TransportChannelHandler`读取的`request`是`RequestMessage`，此请求消息将交给`TransportRequestHandler`，如果是`ResponseMessage`，将交给`TransportResponseHandler`。这两个都继承`org.apache.spark.network.server.MessageHandler`:

```java
package org.apache.spark.network.server;

import org.apache.spark.network.protocol.Message;

/**
 * Handles either request or response messages coming off of Netty. A MessageHandler instance
 * is associated with a single Netty Channel (though it may have multiple clients on the same
 * Channel.)
 */
public abstract class MessageHandler<T extends Message> {
  // 对接收到的单个消息进行处理
  public abstract void handle(T message) throws Exception;

  // 当channel激活时调用
  public abstract void channelActive();

  // 当捕捉到channel发生异常时调用
  public abstract void exceptionCaught(Throwable cause);

  // 当channel非激活时调用
  public abstract void channelInactive();
}
```

### ① `Message`

从`MessageHandler`的泛型定义可以看到，其子类处理的消息都派生自`org.apache.spark.network.protocol.Message`接口:

```java
package org.apache.spark.network.protocol;

/** An on-the-wire transmittable message. */
public interface Message extends Encodable {
    // 返回消息的类型
    Type type();

    // 返回消息的body
    ManagedBuffer body();

    // 用于判断消息的body是否包含在消息的同一帧中
    boolean isBodyInFrame();

    enum Type implements Encodable {
        ChunkFetchRequest(0), 
        ChunkFetchSuccess(1), 
        ChunkFetchFailure(2),
        RpcRequest(3), 
        RpcResponse(4), 
        RpcFailure(5),
        StreamRequest(6), 
        StreamResponse(7), 
        StreamFailure(8),
        OneWayMessage(9), 
        UploadStream(10), 
        User(-1);

        private final byte id;

        Type(int id) {
            assert id < 128 : "Cannot have more than 128 message types";
            this.id = (byte) id;
        }

        public byte id() { return id; }
        @Override public int encodedLength() { return 1; }
        @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

        public static Type decode(ByteBuf buf) {
            byte id = buf.readByte();
            switch (id) {
                case 0: return ChunkFetchRequest;
                case 1: return ChunkFetchSuccess;
                case 2: return ChunkFetchFailure;
                case 3: return RpcRequest;
                case 4: return RpcResponse;
                case 5: return RpcFailure;
                case 6: return StreamRequest;
                case 7: return StreamResponse;
                case 8: return StreamFailure;
                case 9: return OneWayMessage;
                case 10: return UploadStream;
                case -1: throw new IllegalArgumentException("User type messages cannot be decoded.");
                default: throw new IllegalArgumentException("Unknown message type: " + id);
            }
        }
    }
}

// Message的父接口
public interface Encodable {
    int encodedLength();
    void encode(ByteBuf buf);
}
```

消息的实现类都直接或间接实现了`RequestMessage`或`ResponseMessage`接口：

```
RequestMessage (org.apache.spark.network.protocol)
	// 请求获取流的单个块的序列
    ChunkFetchRequest (org.apache.spark.network.protocol)
    // 此消息类型由远程的RPC服务端进行处理，且需要服务端回复
    RpcRequest (org.apache.spark.network.protocol)
    // 此消息类型由远程的RPC服务端进行处理，且不需要服务端回复
    OneWayMessage (org.apache.spark.network.protocol)
    // 向远程的服务发起请求，以获取流式数据
    StreamRequest (org.apache.spark.network.protocol)
    // 发送可以被当做流读取的数据
    UploadStream (org.apache.spark.network.protocol)
ResponseMessage (org.apache.spark.network.protocol)
	// Abstract class for response messages.
    AbstractResponseMessage (org.apache.spark.network.protocol)
    // 处理ChunkFetchRequest成功后返回的消息
    ChunkFetchFailure (org.apache.spark.network.protocol)
    // 处理RpcRequest失败后返回的消息
    RpcFailure (org.apache.spark.network.protocol)
    // 处理StreamRequest失败后返回的消息
    StreamFailure (org.apache.spark.network.protocol)
AbstractMessage (org.apache.spark.network.protocol)
	// 处理RpcRequest成功后返回的消息
    RpcRequest (org.apache.spark.network.protocol)
    SaslMessage (org.apache.spark.network.sasl)
    // 处理StreamRequest成功后返回的消息
    StreamRequest (org.apache.spark.network.protocol)
```

### ② `TransportRequestHandler`

`TransportRequestHandler`用来处理`RequestMessage`：

```java
package org.apache.spark.network.server;

public class TransportRequestHandler extends MessageHandler<RequestMessage> {
    
    private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

    private final Channel channel;
    private final TransportClient reverseClient;

    /** Handles all RPC messages. */
    private final RpcHandler rpcHandler;

    private final StreamManager streamManager;
    private final long maxChunksBeingTransferred;
    private final ChunkFetchRequestHandler chunkFetchRequestHandler;
    
    @Override
    public void handle(RequestMessage request) throws Exception {
        if (request instanceof ChunkFetchRequest) {
            chunkFetchRequestHandler.processFetchRequest(channel, (ChunkFetchRequest) request);
        } else if (request instanceof RpcRequest) {
            processRpcRequest((RpcRequest) request);
        } else if (request instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage) request);
        } else if (request instanceof StreamRequest) {
            processStreamRequest((StreamRequest) request);
        } else if (request instanceof UploadStream) {
            processStreamUpload((UploadStream) request);
        } else {
            throw new IllegalArgumentException("Unknown request type: " + request);
        }
    }

    private void processRpcRequest(final RpcRequest req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
                }
            });
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
            respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        } finally {
            req.body().release();
        }
    }

    private void processOneWayMessage(OneWayMessage req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
        } finally {
            req.body().release();
        }
    }

    ...
}
```

### ③ `TransportResponseHandler`

`TransportResponseHandler`用来处理`ResponseMessage`:

```java
package org.apache.spark.network.client;
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

    private final Channel channel;

    private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

    private final Map<Long, RpcResponseCallback> outstandingRpcs;

    private final Queue<Pair<String, StreamCallback>> streamCallbacks;
    private volatile boolean streamActive;

    /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
    private final AtomicLong timeOfLastRequestNs;

    @Override
    public void handle(ResponseMessage message) throws Exception {
        if (message instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                logger.warn("Ignoring response for block {} from {} since it is not outstanding",
                            resp.streamChunkId, getRemoteAddress(channel));
                resp.body().release();
            } else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
                resp.body().release();
            }
        } else if (message instanceof ChunkFetchFailure) {
            ChunkFetchFailure resp = (ChunkFetchFailure) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
                            resp.streamChunkId, getRemoteAddress(channel), resp.errorString);
            } else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
                    "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
            }
        } else if (message instanceof RpcResponse) {
            RpcResponse resp = (RpcResponse) message;
            RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                            resp.requestId, getRemoteAddress(channel), resp.body().size());
                resp.body().release();
            } else {
                outstandingRpcs.remove(resp.requestId);
                try {
                    listener.onSuccess(resp.body().nioByteBuffer());
                } finally {
                    resp.body().release();
                }
            }
        } else if (message instanceof RpcFailure) {
            RpcFailure resp = (RpcFailure) message;
            RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
                            resp.requestId, getRemoteAddress(channel), resp.errorString);
            } else {
                outstandingRpcs.remove(resp.requestId);
                listener.onFailure(new RuntimeException(resp.errorString));
            }
        } else if (message instanceof StreamResponse) {
            StreamResponse resp = (StreamResponse) message;
            Pair<String, StreamCallback> entry = streamCallbacks.poll();
            if (entry != null) {
                StreamCallback callback = entry.getValue();
                if (resp.byteCount > 0) {
                    StreamInterceptor<ResponseMessage> interceptor = new StreamInterceptor<>(
                        this, resp.streamId, resp.byteCount, callback);
                    try {
                        TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
                            channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
                        frameDecoder.setInterceptor(interceptor);
                        streamActive = true;
                    } catch (Exception e) {
                        logger.error("Error installing stream handler.", e);
                        deactivateStream();
                    }
                } else {
                    try {
                        callback.onComplete(resp.streamId);
                    } catch (Exception e) {
                        logger.warn("Error in stream handler onComplete().", e);
                    }
                }
            } else {
                logger.error("Could not find callback for StreamResponse.");
            }
        } else if (message instanceof StreamFailure) {
            StreamFailure resp = (StreamFailure) message;
            Pair<String, StreamCallback> entry = streamCallbacks.poll();
            if (entry != null) {
                StreamCallback callback = entry.getValue();
                try {
                    callback.onFailure(resp.streamId, new RuntimeException(resp.error));
                } catch (IOException ioe) {
                    logger.warn("Error in stream failure handler.", ioe);
                }
            } else {
                logger.warn("Stream failure with unknown callback: {}", resp.error);
            }
        } else {
            throw new IllegalStateException("Unknown response type: " + message.type());
        }
    }
    
    ...
}
```

# 五、 `RpcHandler`

从`TransportRequestHandler`实现类的`handle()`方法可以看到，服务端处理消息实际上是把消息交给`RpcHandler`处理。`RpcHandler`是一个抽象类，定义了一些RPC处理的规范，代码如下：

```java
package org.apache.spark.network.server;

public abstract class RpcHandler {

    private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();


    public StreamCallbackWithID receiveStream(TransportClient client,
                                              ByteBuffer messageHeader,
                                              RpcResponseCallback callback) {
        throw new UnsupportedOperationException();
    }

    public abstract StreamManager getStreamManager();

    public void receive(TransportClient client, ByteBuffer message) { receive(client, message, ONE_WAY_CALLBACK); }
    public abstract void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback);

    public void channelActive(TransportClient client) { }

    public void channelInactive(TransportClient client) { }

    public void exceptionCaught(Throwable cause, TransportClient client) { }

    private static class OneWayRpcCallback implements RpcResponseCallback {
        private static final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);

        @Override
        public void onSuccess(ByteBuffer response) {
            logger.warn("Response provided for one-way RPC.");
        }

        @Override
        public void onFailure(Throwable e) {
            logger.error("Error response provided for one-way RPC.", e);
        }

    }

}
```

