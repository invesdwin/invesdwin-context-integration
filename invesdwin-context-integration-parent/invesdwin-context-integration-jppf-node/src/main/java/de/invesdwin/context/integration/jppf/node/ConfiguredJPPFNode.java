package de.invesdwin.context.integration.jppf.node;

import java.net.URI;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;
import javax.inject.Named;

import org.jppf.node.NodeRunner;
import org.jppf.server.node.JPPFNode;
import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.ftp.FtpFileChannel;
import de.invesdwin.context.integration.ftp.FtpServerDestinationProvider;
import de.invesdwin.context.integration.jppf.client.JPPFProcessingThreadsCounter;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FTimeUnit;

@Named
@Immutable
public final class ConfiguredJPPFNode implements FactoryBean<JPPFNode>, IStartupHook {

    private static boolean createInstance = true;
    private static JPPFNode instance;

    @Inject
    private FtpServerDestinationProvider ftpServerDestinationProvider;

    private ConfiguredJPPFNode() {}

    @Override
    public JPPFNode getObject() throws Exception {
        return getInstance();
    }

    @Override
    public Class<?> getObjectType() {
        return JPPFNode.class;
    }

    public static synchronized boolean isCreateInstance() {
        return createInstance;
    }

    public static synchronized void setCreateInstance(final boolean createInstance) {
        ConfiguredJPPFNode.createInstance = createInstance;
    }

    public static synchronized JPPFNode getInstance() {
        if (instance == null && createInstance) {
            Assertions.checkTrue(JPPFNodeProperties.INITIALIZED);
            NodeRunner.main("noLauncher");
            instance = (JPPFNode) NodeRunner.getNode();
            Assertions.checkNotNull(instance, "Startup failed!");
        }
        return instance;
    }

    public static synchronized void setInstance(final JPPFNode instance) {
        Assertions.checkNull(ConfiguredJPPFNode.instance);
        ConfiguredJPPFNode.instance = instance;
    }

    @Override
    public void startup() throws Exception {
        if (isCreateInstance()) {
            Assertions.checkNotNull(getInstance());
        }
    }

    @Scheduled(initialDelay = 0, fixedDelay = 1 * FTimeUnit.SECONDS_IN_MINUTE * FTimeUnit.MILLISECONDS_IN_SECOND)
    @SkipParallelExecution
    private void createHeartbeatContent() {
        final JPPFNode node;
        synchronized (ConfiguredJPPFNode.class) {
            node = instance;
        }
        if (node != null) {
            final String nodeUuid = node.getUuid();
            final int processingThreads = JPPFConfiguration.get(JPPFProperties.PROCESSING_THREADS);
            final FDate heartbeat = new FDate();
            final String content = nodeUuid + JPPFProcessingThreadsCounter.FTP_CONTENT_SEPARATOR + processingThreads
                    + JPPFProcessingThreadsCounter.FTP_CONTENT_SEPARATOR
                    + heartbeat.toString(JPPFProcessingThreadsCounter.FTP_CONTENT_DATEFORMAT);
            final URI ftpServerUri = ftpServerDestinationProvider.getDestination();
            try (FtpFileChannel channel = new FtpFileChannel(ftpServerUri,
                    JPPFProcessingThreadsCounter.FTP_DIRECTORY)) {
                channel.setFilename("node_" + nodeUuid + ".heartbeat");
                channel.connect();
                channel.write(content.getBytes());
            }
        }
    }
}
