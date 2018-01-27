# invesdwin-context-integration

This project provides integration modules for the [invesdwin-context](https://github.com/subes/invesdwin-context) module system.

## Maven

Releases and snapshots are deployed to this maven repository:
```
http://invesdwin.de/artifactory/invesdwin-oss-remote
```

Dependency declaration:
```xml
<dependency>
	<groupId>de.invesdwin</groupId>
	<artifactId>invesdwin-context-integration-ws</artifactId>
	<version>1.0.0-SNAPSHOT</version>
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
- **invesdwin-context-integration-ws-registry**: this module provides an embedded registry server. Just create and deploy your own distribution (for an example see the `invesdwin-context-integration-ws-registry-dist` distribution module) and make sure all clients are configured to the appropriate server via the `de.invesdwin.context.integration.ws.IntegrationWsProperties.REGISTRY_SERVER_URI` system property. The registry server is available at the context path `<WEBSERVER_BIND_URI>/spring-web/` of your embedded web server.
- **invesdwin-context-integration-maven**: this module adds support for downloading artifacts from [Maven](https://maven.apache.org/) repositories via [Shrinkwrap Resolver](https://github.com/shrinkwrap/resolver). With this, one can implement dynamic loading of plugins via the web. The following system properties are supported:
```properties
de.invesdwin.context.integration.maven.MavenProperties.LOCAL_REPOSITORY_DIRECTORY=${user.home}/.m2/repository
de.invesdwin.context.integration.maven.MavenProperties.REMOTE_REPOSITORY_1_URL=http://invesdwin.de/artifactory/invesdwin-oss-remote
# add more remote repositories by incrementing the id (though keep them without gaps)
#de.invesdwin.context.integration.maven.MavenProperties.REMOTE_REPOSITORY_2_URL=
```
- **invesdwin-context-integration-affinity**: this module integrates a library to configure the [Java-Thread-Affinity](https://github.com/OpenHFT/Java-Thread-Affinity). It helps to increase throughput for communication threads when a cpu is reserved for a given task by making wakeups on events faster. Though in our experience this increase in latency comes at a cost of throughput depending on the cpu and operating system, because cpu intensive workloads on fixed cores disable the heat and power consumption optimizations of the scheduler algorithm in the operating system. So always test your performance properly before applying a thread affinity. For this purpose you can use `AffinityProperties.setEnabled(boolean)` to enable/disable the affinity during runtime without having to remove the affinity setup code during testing. Please read the original documentation of the library to learn how to adjust your threads to actually apply the affinity.

## Web Concerns

**SECURITY:** You can apply extended security features by utilizing the [invesdwin-context-security-web](https://github.com/subes/invesdwin-context-security) module which allows you to define your own rules as described in the [spring-security namespace configuration](http://docs.spring.io/spring-security/site/docs/current/reference/html/ns-config.html#ns-minimal) documentation. Just define your own rules in your own `<http>` tag for a specific context path in your own custom spring context xml and register it for the invesdwin application bootstrap to be loaded as normally done via an `IContextLocation` bean.

**DEPLOYMENT:** The above context paths are the defaults for the embedded web server as provided by the `invesdwin-context-webserver` module. If you do not want to deploy your web services, servlets or web applications in an embedded web server, you can create distributions that repackage them as WAR files (modifications via maven-overlays or just adding your own web.xml) to deploy in a different server under a specific context path. Or you could even repackage them as an EAR file and deploy them in your application server of choice. Or if you like you could roll your own alternative embedded web server that meets your requirements. Just ask if you need help with any of this.

## Support

If you need further assistance or have some ideas for improvements and don't want to create an issue here on github, feel free to start a discussion in our [invesdwin-platform](https://groups.google.com/forum/#!forum/invesdwin-platform) mailing list.
