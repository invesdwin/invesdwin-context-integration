# invesdwin-context-integration

This project provides integration modules for the [invesdwin-context](https://github.com/subes/invesdwin-context) module system.

## Maven

Releases and snapshots are deployed to this maven repository:
```
http://invesdwin.de/artifactory/invesdwin-oss
```

Dependency declaration:
```xml
<dependency>
	<groupId>de.invesdwin</groupId>
	<artifactId>invesdwin-context-integration</artifactId>
	<version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Integration Module

For applications that also rely on IO (Input/Output of files and streams) and IPC (Inter-Process-Communication), the `invesdwin-context-integration` module provides a few tools that make the life easier here. It integrates [spring-integration](https://projects.spring.io/spring-integration/) which is an optional framework with which you can build a pipes and filters architecture for your integration workflow. Other invesdwin integration modules extend this to provide support for JMS, AMPQ, REST, WebServices, Spring-Batch, Hadoop and so on. This module though only provides the core functionality:
- **integration.log**: per convention we want all our ingoing and outgoing messages to be available in a human readable format to debug complex integration issues. For this normally all messages get logged to `log/integration.log`. You can disable this for improved performance by changing the loglevel for `de.invesdwin.MESSAGES` in your logback config. The `MessageLoggingAspect` intercepts all spring-integration `@Gateway` and `@ServiceActivator` beans, so there is no need for you to figure out how to get those messages.
- **NetworkUtil**: sometimes when accessing web content or consuming remote services over the internet we get some hiccups. Then we want to know if the service has a problem or the internet as a whole is currently down. This can be determined with this util. The appropriate action might be to make the process simply wait for the internet to come back online again before retrying, which can be accomplished with this. Or maybe you need to know your external IP-Address (e.g. to register your service instance with a registry). When you are behind a router this can only be retrieved by asking a remote website like [whatismyip.com](https://www.whatismyip.com/). So we have something similar available here.
- **@Retry**: the RetryAspect retries all method invocations where this annotation is found (alternatively there is the `ARetryingCallable`) depending on the exception that was thrown. If some IO or communication related exception if encountered, the method call is retried with a default backoff policy. You can also throw `RetryLaterException` or `RetryLaterRuntimeException` inside your method to decide for a retry on some other exception or condition.
- **IRetryHook**: this hook interface allows you to abort retries (by throwing another exception in `onBeforeRetry(...)`) or do additional retry logic, like sending out an email to an administrator when a system is down or trying to reinitialize some remote service automatically (e.g. when a dynamic service vanished and you have to switch to a different instance which you rediscover from the central service registry). The `RetryOriginator` object should provide all meta-information needed to decide about special cases in your retry logic.
- **Marshallers**: convert to/from [XML](https://en.wikipedia.org/wiki/XML)/[JSON](https://en.wikipedia.org/wiki/JSON) with [JAXB](https://en.wikipedia.org/wiki/Java_Architecture_for_XML_Binding)/[Jackson](https://github.com/FasterXML/jackson-databind). Initially [gson](https://github.com/google/gson) was used for JSON processing, but jackson was found to be faster by a magnitude and to provide better configurability even though it is a bit less easy or intuitive to use. 
- **CSV**: the `ABeanCsvReader`, `ABeanCsvWriter` and other classes provide some utilities to easily read/write CSV files. This utilizes the popular [spring-batch](http://projects.spring.io/spring-batch/) `FlatFileItemReader` and Mapper functionality. Though a complete spring-batch integration is found in a different invesdwin integration module.

## Other Integration Modules

Other more specific integration modules include:

- **AMQP**: this module allows to connect easily to a [RabbitMQ](https://www.rabbitmq.com/) ([AMQP](https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol)) server via [spring-amqp](https://projects.spring.io/spring-amqp/). The channels are defined in your own spring-integration xml files as can be seen in the respective test cases. There are system properties available to define the connection parameters to the default server (create your own RabbitTemplate instances for additional connections):
```properties
de.invesdwin.context.integration.amqp.AmqpClientProperties.HOST=localhost
# -1 means to use default port
de.invesdwin.context.integration.amqp.AmqpClientProperties.PORT=-1
de.invesdwin.context.integration.amqp.AmqpClientProperties.USER=guest
de.invesdwin.context.integration.amqp.AmqpClientProperties.PASSWORD=guest
```
- **JMS**: this module allows to connect easily to an [ActiveMQ](http://activemq.apache.org/) ([JMS](https://en.wikipedia.org/wiki/Java_Message_Service) server via [spring-integration-jms](http://docs.spring.io/spring-integration/reference/html/jms.html). Currently this module is setup to connect processes in a local [network of brokers](http://activemq.apache.org/networks-of-brokers.html), so right now there are no connection system properties available (but you can still create your own JmsTemplate instances for additional connections). Or just demand an extension to this module to support different operation modes and extended configurability via system properties.
- **Batch**: this module supports writing jobs for [spring-batch](http://projects.spring.io/spring-batch/). It eases integration by automatically populating the batch database tables using [invesdwin-context-persistence-jpa](https://github.com/subes/invesdwin-context-persistence) which even works in test cases with an in memory database. See the included test cases for examples. Place job xml files into the classpath at `/META-INF/batch/ctx.batch.*.xml` so they get automatically discovered during application bootstrap. The `IJobService` provides convenience methods for running and managing these jobs. The `invesdwin-context-integration-batch-admin` module embeds the [spring-batch-admin](http://docs.spring.io/spring-batch-admin/) web frontend into your application. It provides a convenient UI to manually fiddle around with your jobs on an ad-hoc basis.
- **Hadoop**: this module extends the batch module to support for [Hadoop](http://hadoop.apache.org/) via [spring-data-hadoop](http://projects.spring.io/spring-hadoop/). The provisioning of job jars can be optionally simplified by the class `HadoopJobMergedClasspathJar` which creates a jar from your embedded job from the classpath which can then be automatically deployed to your cluster (see the included test cases for an example). The classes `AMapper` and `AReducer` can be used as the base classes for your job implementation when you want them to do an invesdwin application bootstrap too (when they also use invesdwin-context features). You can specify to which cluster to connect to via the following system properties:
```properties
fs.defaultFS=hdfs://localhost:8020
yarn.resourcemanager.hostname=localhost
```
