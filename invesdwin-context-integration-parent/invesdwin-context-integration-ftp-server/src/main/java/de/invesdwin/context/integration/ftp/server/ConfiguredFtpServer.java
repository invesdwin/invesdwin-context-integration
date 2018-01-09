package de.invesdwin.context.integration.ftp.server;

import java.util.Arrays;

import javax.annotation.concurrent.GuardedBy;
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
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
public class ConfiguredFtpServer implements FtpServer {

    private final Log log = new Log(this);

    @GuardedBy("this")
    private FtpServer ftpServer;

    @Override
    public synchronized void start() {
        Assertions.checkNull(ftpServer, "already started");

        log.info("Starting ftp server at: %s", FtpServerProperties.getServerBindUri());
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
        final InMemoryUserManager userManager = new InMemoryUserManager(new Md5PasswordEncryptor(), "admin_disabled");
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

        ftpServer = serverFactory.createServer();

        try {
            ftpServer.start();
        } catch (final FtpException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void stop() {
        if (ftpServer != null) {
            ftpServer.stop();
        }
    }

    @Override
    public synchronized boolean isStopped() {
        if (ftpServer != null) {
            return ftpServer.isStopped();
        } else {
            return true;
        }
    }

    @Override
    public synchronized void suspend() {
        if (ftpServer != null) {
            ftpServer.suspend();
        }
    }

    @Override
    public synchronized void resume() {
        if (ftpServer != null) {
            ftpServer.resume();
        }
    }

    @Override
    public synchronized boolean isSuspended() {
        if (ftpServer != null) {
            return ftpServer.isSuspended();
        } else {
            return false;
        }
    }

}
