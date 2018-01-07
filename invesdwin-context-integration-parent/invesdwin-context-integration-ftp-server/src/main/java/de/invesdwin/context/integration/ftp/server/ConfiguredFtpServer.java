package de.invesdwin.context.integration.ftp.server;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
@Named
public class ConfiguredFtpServer implements FactoryBean<FtpServer>, IStartupHook {

    private static FtpServer instance;

    public static synchronized FtpServer getInstance() {
        if (instance == null) {
            final FtpServerFactory serverFactory = new FtpServerFactory();
            final ConnectionConfigFactory connectionConfig = new ConnectionConfigFactory();
            connectionConfig.setAnonymousLoginEnabled(false);
            connectionConfig.setMaxThreads(50);
            serverFactory.setConnectionConfig(connectionConfig.createConnectionConfig());
            final ListenerFactory factory = new ListenerFactory();
            // set the port of the listener
            factory.setPort(FtpServerProperties.PORT);
            // replace the default listener
            serverFactory.addListener("default", factory.createListener());
            instance = serverFactory.createServer();
            try {
                instance.start();
            } catch (final FtpException e) {
                throw new RuntimeException(e);
            }
        }
        return instance;
    }

    @Override
    public FtpServer getObject() throws Exception {
        return getInstance();
    }

    @Override
    public Class<?> getObjectType() {
        return FtpServer.class;
    }

    @Override
    public void startup() throws Exception {
        Assertions.checkNotNull(getInstance());
    }

}
