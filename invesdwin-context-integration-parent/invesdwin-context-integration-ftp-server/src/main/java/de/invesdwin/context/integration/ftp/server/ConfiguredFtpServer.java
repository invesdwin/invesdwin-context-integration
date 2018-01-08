package de.invesdwin.context.integration.ftp.server;

import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;

import de.invesdwin.context.integration.ftp.FtpClientProperties;
import de.invesdwin.context.integration.ftp.server.internal.InMemoryUserManager;

@ThreadSafe
public class ConfiguredFtpServer implements FtpServer {

    private FtpServer delegate;

    private synchronized FtpServer getDelegate() {
        if (delegate == null) {
            final FtpServerFactory serverFactory = new FtpServerFactory();

            //disable anonymous access
            final ConnectionConfigFactory connectionConfig = new ConnectionConfigFactory();
            connectionConfig.setAnonymousLoginEnabled(false);
            connectionConfig.setMaxThreads(FtpServerProperties.MAX_THREADS);
            serverFactory.setConnectionConfig(connectionConfig.createConnectionConfig());

            // replace the default listener port
            final ListenerFactory factory = new ListenerFactory();
            factory.setPort(FtpServerProperties.PORT);
            serverFactory.addListener("default", factory.createListener());

            // create user
            final InMemoryUserManager userManager = new InMemoryUserManager(new Md5PasswordEncryptor(),
                    "admin_disabled");
            final BaseUser user = new BaseUser();
            user.setName(FtpClientProperties.USERNAME);
            user.setPassword(FtpClientProperties.PASSWORD);
            user.setEnabled(true);
            user.setHomeDirectory(FtpServerProperties.WORKING_DIR.getAbsolutePath());
            user.setAuthorities(Arrays.asList(new WritePermission()));
            try {
                userManager.save(user);
            } catch (final FtpException e) {
                throw new RuntimeException(e);
            }
            serverFactory.setUserManager(userManager);

            delegate = serverFactory.createServer();

        }
        return delegate;
    }

    @Override
    public void start() {
        try {
            getDelegate().start();
        } catch (final FtpException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        getDelegate().stop();
    }

    @Override
    public boolean isStopped() {
        return getDelegate().isStopped();
    }

    @Override
    public void suspend() {
        getDelegate().suspend();
    }

    @Override
    public void resume() {
        getDelegate().resume();
    }

    @Override
    public boolean isSuspended() {
        return getDelegate().isSuspended();
    }

}
