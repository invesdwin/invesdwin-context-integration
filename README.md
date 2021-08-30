# invesdwin-context-integration

This project provides integration modules for the [invesdwin-context](https://github.com/subes/invesdwin-context) module system.

## Maven

Releases and snapshots are deployed to this maven repository:
```
https://invesdwin.de/repo/invesdwin-oss-remote/
```

Dependency declaration:
```xml
<dependency>
	<groupId>de.invesdwin</groupId>
	<artifactId>invesdwin-context-integration-ws</artifactId>
	<version>1.0.2</version><!---project.version.invesdwin-context-integration-parent-->
</dependency>
```

## Integration Modules

The core integration module resides in the [invesdwin-context](https://github.com/subes/invesdwin-context/blob/master/README.md#integration-module) repository. You can read more about it there. The modules here build on top of it to provide more specific integration functionality:

- **invesdwin-context-integration-amqp**: this module allows to connect easily to a [RabbitMQ](https://www.rabbitmq.com/) ([AMQP](https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol)) server via [spring-amqp](https://projects.spring.io/spring-amqp/). The channels are defined in your own spring-integration xml files as can be seen in the respective test cases. There are system properties available to define the connection parameters to the default server (create your own RabbitTemplate instances for additional connections):
```properties
de.invesdwin.context.integration.amqp.AmqpClientProperties.HOST=localhost
# -1 means to use default port
de.invesdwin.context.integration.amqp.AmqpClientProperties.PORT=-1
de.invesdwin.context.integration.amqp.AmqpClientProperties.USER=guest
de.invesdwin.context.integration.amqp.AmqpClientProperties.PASSWORD=guest
```
- **invesdwin-context-integration-jms**: this module allows to connect easily to an [ActiveMQ](http://activemq.apache.org/) ([JMS](https://en.wikipedia.org/wiki/Java_Message_Service) server via [spring-integration-jms](http://docs.spring.io/spring-integration/reference/html/jms.html). Currently this module is setup to connect processes in a local [network of brokers](http://activemq.apache.org/networks-of-brokers.html), so right now there are no connection system properties available (but you can still create your own JmsTemplate instances for additional connections). Or just demand an extension to this module to support different operation modes and extended configurability via system properties.
- **invesdwin-context-integration-batch**: this module supports writing jobs for [spring-batch](http://projects.spring.io/spring-batch/). It eases integration by automatically populating the batch database tables using [invesdwin-context-persistence-jpa](https://github.com/subes/invesdwin-context-persistence) which even works in test cases with an in memory database. See the included test cases for examples. Place job xml files into the classpath at `/META-INF/batch/ctx.batch.*.xml` so they get automatically discovered during application bootstrap. The `IJobService` provides convenience methods for running and managing these jobs. The `invesdwin-context-integration-batch-admin` module embeds the [spring-batch-admin](http://docs.spring.io/spring-batch-admin/) web frontend into your application. It provides a convenient UI to manually fiddle around with your jobs on an ad-hoc basis (it uses the context path root at `<WEBSERVER_BIND_URI>/` of your embedded web server).
- **invesdwin-context-integration-hadoop**: this module extends the batch module to support for [Hadoop](http://hadoop.apache.org/) via [spring-data-hadoop](http://projects.spring.io/spring-hadoop/). The provisioning of job jars can be optionally simplified by the class `HadoopJobMergedClasspathJar` which creates a jar from your embedded job from the classpath which can then be automatically deployed to your cluster (see the included test cases for an example). The classes `AMapper` and `AReducer` can be used as the base classes for your job implementation when you want them to do an invesdwin application bootstrap too (when they also use invesdwin-context features). You can specify to which cluster to connect to via the following system properties:
```properties
fs.defaultFS=hdfs://localhost:8020
yarn.resourcemanager.hostname=localhost
```
- **invesdwin-context-integration-ws**: this module adds support for [RESTful](https://en.wikipedia.org/wiki/Representational_state_transfer) web services via [spring-web](https://docs.spring.io/spring/docs/current/spring-framework-reference/html/mvc.html#mvc-ann-controller). Just define a bean and annotate it with `@Controller` for it to be automatically available under the context path `<WEBSERVER_BIND_URI>/spring-web/...` of your embedded web server. Also this module adds in support for a web service registry via the `IRegistryService` class which connects as a client to your own registry server (read more on that in the WS-Registry topic later). Lastly this module also adds support for [SOAP](https://en.wikipedia.org/wiki/SOAP) web services via [spring-ws](http://docs.spring.io/spring-ws/site/reference/html/what-is-spring-ws.html). This allows for easy use of enterprise web services as spring integration channels (the context path is `<WEBSERVER_BIND_URI>/spring-web/...` of your embedded web server). You can register your own web services in the registry server via the classes `RestWebServicePublication` for REST and `XsdWebServicePublication` for SOAP respectively. The class `RegistryDestinationProvider` can be used to bind to an external web service that is looked up via the registry server. The following system properties are available to change your registry server url and to change the credentials for your web service security (message encryption is disabled per default since you should rather use an SSL secured web server (maybe even a proxy) for this as most other methods are slower and more complicated in comparison):
```properties
# it makes sense to put these settings into the $HOME/.invesdwin/system.properties file so they apply to all processes (alternatively override these in your distribution modules)
de.invesdwin.context.integration.ws.IntegrationWsProperties.REGISTRY_SERVER_URI=http://please.override.this/spring-web
de.invesdwin.context.integration.ws.IntegrationWsProperties.WSS_USERNAMETOKEN_USER=invesdwin
de.invesdwin.context.integration.ws.IntegrationWsProperties.WSS_USERNAMETOKEN_PASSWORD=invesdwin
de.invesdwin.context.integration.ws.IntegrationWsProperties.SPRING_WEB_USER=invesdwin
de.invesdwin.context.integration.ws.IntegrationWsProperties.SPRING_WEB_PASSWORD=invesdwin
```
- **invesdwin-context-integration-ws-jaxrs**: this module adds support for [JAX-RS](https://en.wikipedia.org/wiki/Java_API_for_RESTful_Web_Services) via the [Jersey](https://jersey.java.net/) reference implementation. Just annotate your `@Named` spring beans additionally with `@Path` to make expose them via JAX-RS as RESTful web services (the context path is `<WEBSERVER_BIND_URI>/jersey/...` of your embedded web server).
- **invesdwin-context-integration-ws-jaxws**: this module adds support for [JAX-WS](https://en.wikipedia.org/wiki/Java_API_for_XML_Web_Services) via the [Apache CXF](http://cxf.apache.org/) implementation. Just define your web services as you would [normally do it with CFX and spring](http://cxf.apache.org/docs/writing-a-service-with-spring.html) inside a context xml file that gets registered in an `IContextLocation` for the invesdwin-context application bootstrap (the context path is `<WEBSERVER_BIND_URI>/cxf/...` of your embedded web server).
- **invesdwin-context-integration-ws-registry**: this module provides an embedded registry server. Just create and deploy your own distribution (for an example see the `invesdwin-context-integration-ws-registry-dist` distribution module) and make sure all clients are configured to the appropriate server via the `de.invesdwin.context.integration.ws.IntegrationWsProperties.REGISTRY_SERVER_URI` system property. The registry server is available at the context path `<WEBSERVER_BIND_URI>/spring-web/registry/` of your embedded web server.
- **invesdwin-context-integration-maven**: this module adds support for downloading artifacts from [Maven](https://maven.apache.org/) repositories via [Shrinkwrap Resolver](https://github.com/shrinkwrap/resolver). With this, one can implement dynamic loading of plugins via the web. The following system properties are supported:
```properties
de.invesdwin.context.integration.maven.MavenProperties.LOCAL_REPOSITORY_DIRECTORY=${user.home}/.m2/repository
de.invesdwin.context.integration.maven.MavenProperties.REMOTE_REPOSITORY_1_URL=http://invesdwin.de/artifactory/invesdwin-oss-remote
# add more remote repositories by incrementing the id (though keep them without gaps)
#de.invesdwin.context.integration.maven.MavenProperties.REMOTE_REPOSITORY_2_URL=
```
- **invesdwin-context-integration-affinity**: this module integrates a library to configure the [Java-Thread-Affinity](https://github.com/OpenHFT/Java-Thread-Affinity). It helps to decrease latency for communication threads when a cpu is reserved for a given task by making wakeups on events faster. Though in our experience this decrease in latency comes at a cost of throughput depending on the cpu and operating system, because cpu intensive workloads on fixed cores disable the heat and power consumption optimizations of the scheduler algorithm in the operating system. So always test your performance properly before applying a thread affinity. For this purpose you can use `AffinityProperties.setEnabled(boolean)` to enable/disable the affinity during runtime without having to remove the affinity setup code during testing. Please read the original documentation of the library to learn how to adjust your threads to actually apply the affinity.
- **invesdwin-context-integration-jppf**: this is the client module for [JPPF](http://www.jppf.org/) which is a library for grid computing in Java. It allows flexible topologies consisting of nodes (worker processes) and drivers (server which coordinates jobs to run on nodes) and employs a remote classloader connection to comfortably transmit jobs for execution on nodes without having to deploy code manually. This modules configures JPPF for speed and reliability out of the box. It also adds a basic authentication mechanism to prevent your grid from accepting connections from unauthorized sources. The multicast discovery is disabled for security reasons and instead the JPPF server discovery is handled via ws-registry lookups. The `ConfiguredJPPFClient` provides access to submitting jobs and among other things a `JPPFProcessingThreadsCounter` which gives reliable information about how many drivers, nodes and processing threads are available in your grid. Also you can override any [JPPF internal properties](http://www.jppf.org/doc/5.2/index.php?title=Configuration_properties_reference) via the system properties override mechanisms provided by [invesdwin-context](https://github.com/subes/invesdwin-context#tools). Though the only properties you should worry about changing would be the following ones:
```properties
# replace this with a secret token for preventing public access
de.invesdwin.context.integration.jppf.JPPFClientProperties.USERNAME_TOKEN=invesdwin
# enable local execution, disable it if you have a node running on this computer
jppf.local.execution.enabled=true
```
- **invesdwin-context-integration-jppf-server**: this module handles the setup of a JPPF driver instance. You can enable it in your unit tests via the `@JPPFServerTest` annotation. You need at least one server instance deployed in your grid. Though if multiple ones are deployed, they will add the benefit of improved reliability and more dynamic load balancing despite allowing more flexible control over the topology, since not all clients or nodes might be able to reach each other when multiple networks and firewalls are involved. In that case server instances can operate as bridges. The interesting properties here are:
```properties
# a local node reduces the overhead significantly, though you should disable it if a node is deployed on the same server or then when server should not execute jobs
jppf.local.node.enabled=true
# per default a random port will be picked
jppf.server.port=0
jppf.management.port=0
```
- **invesdwin-context-integration-jppf-node**: the node is a worker process that executes tasks which are bundled as jobs in JPPF. The configured load balancing mechanism will assign as many tasks per node as there are processing threads available for each. You can use the `@JPPFNodeTest` annotation to enable a node in your unit tests. The node will cycle through the servers that were returned by a ws-registry lookup and connect to the first one that can be reached properly. If you want to reduce the overhead of the remote classloader a bit you can either deploy whole jars with your jobs (via the [JPPF ClassPath](http://www.jppf.org/doc/5.2/index.php?title=Job_Service_Level_Agreement#Setting_a_class_path_onto_the_job) mechanism) or directly deploying the most common classes embedded in your nodes JVM classpath. If you require a spring context in your tasks, you have to initialize and tear it down inside the task properly. If you require access to invesdwin modules that are accessible via the [MergedContext](https://github.com/subes/invesdwin-context#tools), then you have to deploy them alongside your node module or use an isolated classloader in your task which initializes its own context. Nodes will publish their metadata to an FTP server (those modules will be explained later) so that counting nodes and processing threads is a bit more reliable and works even with offline nodes (nodes that are behind firewalls and thus invisible through JMX or which are explicitly in [offline mode](http://www.jppf.org/doc/5.2/index.php?title=Offline_nodes)). These counts can then be used for manually splitting workloads over multiple jobs. For example when you want to reduce the JPPF overhead (of remote classloading and context setup) or even the context switches of threads inside the nodes, you can submit jobs that have only one task that contains a chunked bit of work. This chunked bit of work will be split among the available threads in the node inside the task (manually by your code) and then processed chunk-wise without the thread needing to pick its next work item, since all of them were given to it in the beginning. The task waits for all the work to be finished in order to compress and upload the results as a whole (which also saves time when uploading results). Since JPPF nodes can only process one job at a time (though multiple tasks in a job are handled more efficiently than multiple jobs; this limitation is due to potential issues with static fields and native libraries you might encounter when mixing various different tasks without knowing what they might do), this allows you more fine grained control to extract the most performance from your nodes depending on your workload. When you submit lots of jobs from your client, it will make sure that there are enough connections available for each job by scaling connections up dynamically within limits (because each job requires its separate connection in JPPF). This solves some technical limitations of JPPF for the benefit of staying flexible.
- **invesdwin-context-integration-jppf-admin**: JPPF also provides an [Admin Console UI](http://www.jppf.org/screenshots/) which is integrated via this module. It shows health statistics on the servers and nodes and allows to manage their settings. We have only integrated the Desktop Admin Console here since the Web Admin Console is still under heavy development as of this time. When it is finished, we might provide a module for that as well.
- **invesdwin-context-integration-ftp**: here you find [ftp4j](http://www.sauronsoftware.it/projects/ftp4j/) as a client library for FTP access. Usage is simplified by `FtpFileChannel` which provides a way to transmit information for a communication channel for FTP transfers by serializing the object. This is useful for JPPF because you can get faster result upload speeds by uploading to FTP and sending the `FtpFileChannel` as a result of the computation to the client, which then downloads the results from the FTP server. By utilizing `AsyncFileChannelUpload` you can even make this process asynchronous which enables the JPPF computation node to continue with the next task while uploading the results in parallel. The client which utilizes `AsyncFileChannelDownload` will wait until the upload is finished by the node to start the download. Timeouts and retries are being applied to handle any failures that might happen. Though if this fails completely, the client should resubmit the job. The following system properties are available to configure the FTP credentials (you can override the `FtpFileChannel.login()` method to use different credentials; FTP server discovery is supposed to happen via `FtpServerDestinationProvider` as a ws-registry lookup):
```properties
de.invesdwin.context.integration.ftp.FtpClientProperties.USERNAME=invesdwin
de.invesdwin.context.integration.ftp.FtpClientProperties.PASSWORD=invesdwin
```
- **invesdwin-context-integration-ftp-server**: this is an embedded FTP server which is provided by [Apache MINA FtpServer](https://mina.apache.org/ftpserver-project/). As usual you can annotate your tests with `@FtpServerTest` to enable the server in your unit tests. The following system properties are available:
```properties
de.invesdwin.context.integration.ftp.server.FtpServerProperties.PORT=2221
de.invesdwin.context.integration.ftp.server.FtpServerProperties.MAX_THREADS=200
# set to clean the server directory regularly of old files, keep empty or unset to disable this feature
de.invesdwin.context.integration.ftp.server.FtpServerProperties.PURGE_FILES_OLDER_THAN_DURATION=1 DAYS
```
- **invesdwin-context-integration-webdav**: since FTP has a high protocol overhead when being used for lots of short lived connections, we also provide a more lightweight alternative via WebDAV for file transmissions. It works over HTTP so it is designed for low protocol overhead since it reduces the amount of round trips needed and does not require a socket connection to be kept alive between requests. The client library in use is [sardine](https://github.com/lookfirst/sardine) which is available as a `WebdavFileChannel` for use in `AsyncFileChannelUpload` and `AsyncFileChannelDownload`. The following system properties are available to configure the WebDAV credentials (you can override the `WebdavFileChannel.login()` method to use different credentials; WebDAV server discovery is supposed to happen via `WebdavServerDestinationProvider` as a ws-registry lookup):
```properties
de.invesdwin.context.integration.webdav.WebdavClientProperties.USERNAME=invesdwin
de.invesdwin.context.integration.webdav.WebdavClientProperties.PASSWORD=invesdwin
```
- **invesdwin-context-integration-webdav-server**: this is an embedded WebDAV server which is provided by [WebDAV-Handler](https://github.com/Commonjava/webdav-handler). It is a simple implementation that also provides support for advanced WebDAV features like file locking. As usual you can annotate your tests with `@WebserverTest` when using `invesdwin-context-webserver` to enable the server in your unit tests (the context path is `<WEBSERVER_BIND_URI>/webdav/` of your embedded web server). The following system properties are available:
```properties
# set to clean the server directory regularly of old files, keep empty or unset to disable this feature
de.invesdwin.context.integration.webdav.server.WebdavServerProperties.PURGE_FILES_OLDER_THAN_DURATION=1 DAYS
```

## Synchronous Channels

Quote from [golang](https://go.dev/blog/codelab-share): *Do not communicate by sharing memory; instead, share memory by communicating.*

The abstraction in **invesdwin-context-integration-channel** has its origin in the [invesdwin-context-persistence-timeseries](https://github.com/invesdwin/invesdwin-context-persistence#timeseries-module) module. Though it can be used for inter-network, inter-process and inter-thread communication. The idea is to use the channel implementation that best fits the task at hand (heap/off-heap, by-reference/by-value, blocking/non-blocking, bounded/unbounded, queued/direct, durable/transient, publish-subscribe/peer-to-peer, unicast/multicast, client/server, master/slave) with the transport that is most efficient or useful at that spot. To switch to a different transport, simply replace the channel implementation without having to modify the code that uses the channel. Best effort was made to keep the implementations zero-copy and zero-allocation (where possible). If you find ways to further improve the speed, let us know!

- **ISynchronousChannel**: since LevelDB by design supports only one process that accesses the database (even though it does multithreading properly) the developers say one should build their own server frontend around the database to access it from other processes. This makes also sense in order to have only one process that keeps the database updated with expiration checks and a single instance of `ATimeSeriesUpdater`. Though this creates the problem of how to build an IPC (Inter-Process-Communication) mechanism that does not slow down the data access too much. As a solution, this module provides a unidirectional channel (you should use one channel for requests and a separate one for responses on a peer to peer basis) for blazing fast IPC on a single computer with the following tools:
	- **Named Pipes**: the classes `PipeSynchronousReader` and `PipeSynchronousWriter` provide implementations for classic FIFO/Named Pipes
		- `SynchronousChannels.createNamedPipe(...)` creates pipes files with the command line tool `mkfifo`/`mknod` on Linux/MacOSX, returns false when it failed to create one. On linux you have to make sure to open each pipe with a reader/writer in the correct order on both ends or else you will get a [deadlock because it blocks](http://stackoverflow.com/questions/2246862/not-able-to-read-from-named-pipe-in-java). The correct way to do this is to first initialize the request channel on both ends, then the response channel (or vice versa).
		- `SynchronousChannels.isNamedPipeSupported(...)` tells you if those are supported so you can fallback to a different implementation e.g. on Windows (though you could write your own mkfifo.exe/mknod.exe implementation and put it into PATH to make this work with Windows named pipes. At least currently there is no command line tool directly available for this on Windows, despite [makepipe](https://support.microsoft.com/en-us/kb/68941) which might be bundled with some versions of SQL Server, but we did not try to implement this because there is a better alternative to named pipes below that works on any operating system)
		- this implementation only supports one reader and one writer per channel, since all data gets consumed by the reader
	- **Memory Mapping**: the classes `MappedSynchronousReader` and `MappedSynchronousWriter` provide channel implementations that are based on memory mapped files (inspired by [Chronicle-Queue](https://github.com/OpenHFT/Chronicle-Queue) and [MappedBus](https://github.com/caplogic/Mappedbus) while making it even simpler for maximum speed). This implementation has the benefit of being supported on all operating systems. It has less risk of accidentally blocking when things go really wrong because it is non-blocking by nature. And this implementation is slightly faster than Named Pipes for the workloads we have tested, but your mileage might vary and you should test it yourself. Anyway you should still keep communication to a minimum, thus making chunks large enough during iteration to reduce synchronization overhead between the processes, for this a bit of fine tuning on your communication layer might be required.
		- for determining where to place the memory mapped files: `SynchronousChannels.getTmpfsFolderOrFallback()` gives the `/dev/shm` folder when available or the temp directory as a fallback for e.g. Windows. Using [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) could improve the throughput a bit, since the OS will then not even try to flush the memory mapped file to disk.
		- this implementation supports only one writer and theoretically supports multiple readers for a publish/subscribe or multicast scenario of pushing data to clients (e.g. a price feed). But it does not synchronize the reads and the writer might overwrite data while one of the readers still reads it. You would have to implement some sort of synchronization between the processes to prevent this. Another problem might be the synchronous nature of there always being only one message in the channel, clients might miss a few messages if they are not fast enough. A better solution for use-cases like this would be to use [MappedBus](https://github.com/caplogic/Mappedbus) or [Chronicle-Queue](https://github.com/OpenHFT/Chronicle-Queue) directly. Those solutions provide more than unidirectional peer to peer channels and can synchronize multiple readers and writers. For comparison, this implementation does not want to pay the overhead price of synchronization and aims to provide the fastest unidirectional peer to peer channel possible.
	- **Socket**: the classes `SocketSynchronousReader` and `SocketSynchronousWriter` provide implementations for TCP/IP socket communication for when you actually have to go across computers on a network. If you want to use UDP/IP you can use the classes `DatagramSocketSynchronousReader` and `DatagramSocketSynchronousWriter`, but beware that packets can get lost when using UDP, so you need to implement retry behaviours on your client/server. It is not recommended to use these implementations for IPC on localhost, since they are quite slow in comparison to other implementations.
	- **Queue**: the classes `QueueSynchronousReader` and `QueueSynchronousWriter` provide implementations for non-blocking `java.util.Queue` usage for inter-thread-communication inside the JVM, e.g. for using `LinkedBlockingQueue`. The classes `BlockingQueueSynchronousReader` and `BlockingQueueSynchronousWriter` provide an alternative for blocking `java.util.concurrent.BlockingQueue` implementations like `SynchronousQueue` which do not support non-blocking usage. Be aware that not all queue implementations are thread safe, so you might want to synchronize the channels via `SynchronousChannels.synchronize(...)`. Since it might be unfortunate to use the serialization with ISynchronousQueue inside the JVM on queues, this should only be considered as a fallback solution for restricted environments where no faster alternative implementation is supported (e.g. java applets with tight security constraints). Using queues directly with your objects should yield better performance though, if you can live with pass-by-reference or work around that yourself.
- **ASpinWait**:  the channel implementations are non-blocking by themselves (Named Pipes are normally blocking, but the implementation uses them in a non-blocking way, while Memory Mapping is per default non-blocking). This causes the problem of how one should wait on a reader without causing delays from sleeps or causing CPU load from spinning. This problem is solved by `ASpinWait`. It first spins when things are rolling and falls back to a fast sleep interval when communication has cooled down (similar to what `java.util.concurrent.SynchronousQueue` does between threads). The actual timings can be fully customized. To use this class, just override the `isConditionFulfilled()` method by calling `reader.hasNext()`.
- **Performance**: here are some performance measurements against in-process queue implementations using one channel for requests and another separate one for responses, thus each record (of 10,000,000), each involving two messages (becoming 20,000,000), is passed between two threads (one simulating a server and the other one a client). This shows that memory mapping might even be useful as a faster alternative to queues for inter-thread-communication besides it being designed for inter-process-communication (as long as the serialization is cheap):

Old Benchmarks (2016, Core i7-4790K with SSD, Java 8):
```
DatagramSocket (loopback)  Records:   111.01/ms  in  90078 ms    => ~60% slower than Named Pipes
ArrayDeque (synced)        Records:   127.26/ms  in  78579 ms    => ~50% slower than Named Pipes
Named Pipes                Records:   281.15/ms  in  35568 ms    => using this as baseline
SynchronousQueue           Records:   924.90/ms  in  10812 ms    => ~3 times faster than Named Pipes
LinkedBlockingQueue        Records:  1988.47/ms  in   5029 ms    => ~7 times faster than Named Pipes
Mapped Memory              Records:  3214.40/ms  in   3111 ms    => ~11 times faster than Named Pipes
Mapped Memory (tmpfs)      Records:  4237.29/ms  in   2360 ms    => ~15 times faster than Named Pipes
```
New Benchmarks (2021, Core i9-9900k with SSD, Java 16; best in class marked by `*`):
```
Network    CzmqTcp                                Records:    35.24/ms    => ~67% slower
Process    JeromqIpc                              Records:    39.31/ms    => ~63% slower
Network    JzmqTcp                                Records:    39.37/ms    => ~63% slower
Network    JeromqTcp                              Records:    39.84/ms    => ~63% slower
Process    CzmqIpc                                Records:    45.13/ms    => ~58% slower
Process    JzmqIpc                                Records:    55.35/ms    => ~48% slower
Network    KryonetTcp                             Records:    62.71/ms    => ~41% slower
Network    KryonetUdp                             Records:    86.14/ms    => ~19% slower
Network    Socket (loopback)                      Records:   106.51/ms    => using this as baseline
Network    DatagramSocket (loopback)              Records:   113.16/ms    => ~6% faster (unreliable)
Network    SocketChannel (loopback)               Records:   125.85/ms    => ~18% faster
Thread     CzmqInproc                             Records:   132.52/ms    => ~24% faster
Network*   AeronUDP (loopback)                    Records:   176.53/ms    => ~65% faster
Network    DatagramChannel (loopback)             Records:   242.82/ms    => ~2.3 times faster (unreliable)
Process    ChronicleQueue                         Records:   275.52/ms    => ~2.6 times faster
Process    ChronicleQueue (tmpfs)                 Records:   282.06/ms    => ~2.65 times faster
Thread     JeromqInproc                           Records:   296.34/ms    => ~2.8 times faster
Process    NamedPipe (Streaming)                  Records:   350.07/ms    => ~3.3 times faster
Process    NamedPipe (FileChannel)                Records:   425.64/ms    => ~4 times faster
Thread     LockedReference                        Records:   782.04/ms    => ~7.3 times faster
Process    Jocket                                 Records:  1204.82/ms    => ~11.3 times faster (unstable, crashes after a few million messages)
Thread     ArrayBlockingQueue                     Records:  1535.72/ms    => ~14.4 times faster
Thread     LinkedBlockingQueue                    Records:  1716.12/ms    => ~16.1 times faster
Thread     ArrayDeque (synced)                    Records:  1754.14/ms    => ~16.4 times faster
Thread     SynchronizedReference                  Records:  1992.79/ms    => ~18.7 times faster
Thread     SynchronousQueue                       Records:  2207.46/ms    => ~20.7 times faster
Thread     LmaxDisruptorQueue                     Records:  2646.55/ms    => ~24.8 times faster
Thread     LinkedTransferQueue                    Records:  2812.62/ms    => ~26.4 times faster
Thread     AtomicReference                        Records:  2991.95/ms    => ~28.1 times faster
Thread     VolatileReference                      Records:  3268.72/ms    => ~30.7 times faster
Thread     ConversantDisruptorConcurrent          Records:  3389.03/ms    => ~31.8 times faster
Thread     AgronaManyToOneRingBuffer              Records:  3412.39/ms    => ~32 times faster
Thread     ConversantDisruptorBlocking            Records:  3498.95/ms    => ~32.85 times faster
Thread     ConversantPushPullBlocking             Records:  3517.41/ms    => ~33 times faster
Thread     JctoolsSpscLinkedAtomic                Records:  3762.94/ms    => ~35.3 times faster
Thread     ConversantPushPullConcurrent           Records:  3777.29/ms    => ~35.5 times faster
Thread     JctoolsSpscLinked                      Records:  3885.46/ms    => ~36.5 times faster
Thread     JctoolsSpscArray                       Records:  4145.59/ms    => ~38.9 times faster
Thread     AgronaManyToOneQueue                   Records:  4178.33/ms    => ~39.2 times faster
Process    AeronIPC                               Records:  4209.11/ms    => ~39.5 times faster
Thread     AgronaBroadcast                        Records:  4220.66/ms    => ~39.6 times faster
Thread     AgronaManyToManyQueue                  Records:  4235.49/ms    => ~39.8 times faster
Thread     JctoolsSpscAtomicArray                 Records:  4368.72/ms    => ~41 times faster
Thread*    AgronaOneToOneQueue                    Records:  4377.52/ms    => ~41.1 times faster (works with plain objects)
Thread     AgronaOneToOneRingBuffer (ZeroCopy)    Records:  4507.35/ms    => ~42.3 times faster
Thread     AgronaOneToOneRingBuffer (SafeCopy)    Records:  4831.85/ms    => ~45.3 times faster
Process    Mapped Memory                          Records:  6378.77/ms    => ~59.9 times faster
Process*   Mapped Memory (tmpfs)                  Records:  6711.41/ms    => ~63 times faster
```
- **Dynamic Client/Server**: you could utilize RMI with its service registry on localhost  (or something similar) to make processes become master/slave dynamically with failover when the master process exits. Just let each process race to become the master (first one wins) and let all other processes fallback to being slaves and connecting to the master. The RMI service provides mechanisms to setup the synchronous channels (by handing out pipe files) and the communication will then continue faster via your chosen channel implementation (RMI is slower because it uses the default java serialization and the TCP/IP communication causes undesired overhead). When the master process exits, the clients should just race again to get a new master nominated. To also handle clients disappearing, one should implement timeouts via a heartbeat that clients regularly send to the server to detect missing clients and a response timeout on the client so it detects a missing server. This is just for being bullet-proof, the endspoints should normally notify the other end when they close a channel, but this might fail when a process exits abnormally (see [SIGKILL](https://en.wikipedia.org/wiki/Unix_signal#SIGKILL)).

## Web Concerns

**SECURITY:** You can apply extended security features by utilizing the [invesdwin-context-security-web](https://github.com/subes/invesdwin-context-security) module which allows you to define your own rules as described in the [spring-security namespace configuration](http://docs.spring.io/spring-security/site/docs/current/reference/html/ns-config.html#ns-minimal) documentation. Just define your own rules in your own `<http>` tag for a specific context path in your own custom spring context xml and register it for the invesdwin application bootstrap to be loaded as normally done via an `IContextLocation` bean.

**DEPLOYMENT:** The above context paths are the defaults for the embedded web server as provided by the `invesdwin-context-webserver` module. If you do not want to deploy your web services, servlets or web applications in an embedded web server, you can create distributions that repackage them as WAR files (modifications via maven-overlays or just adding your own web.xml) to deploy in a different server under a specific context path. Or you could even repackage them as an EAR file and deploy them in your application server of choice. Or if you like you could roll your own alternative embedded web server that meets your requirements. Just ask if you need help with any of this.

## Support

If you need further assistance or have some ideas for improvements and don't want to create an issue here on github, feel free to start a discussion in our [invesdwin-platform](https://groups.google.com/forum/#!forum/invesdwin-platform) mailing list.
