package de.invesdwin.context.integration.ftp.server;

import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.ftp.FtpClientProperties;
import de.invesdwin.context.integration.ftp.server.internal.InMemoryUserManager;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
@Named
public class ConfiguredFtpServer implements FactoryBean<FtpServer>, IStartupHook {

    private static FtpServer instance;

    public static synchronized FtpServer getInstance() {
        if (instance == null) {
            final FtpServerFactory serverFactory = new FtpServerFactory();

            //disable anonymous access
            final ConnectionConfigFactory connectionConfig = new ConnectionConfigFactory();
            connectionConfig.setAnonymousLoginEnabled(false);
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
            user.setHomeDirectory(FtpServerProperties.BASE_DIRECTORY.getAbsolutePath());
            user.setAuthorities(Arrays.asList(new WritePermission()));
            try {
                userManager.save(user);
            } catch (final FtpException e) {
                throw new RuntimeException(e);
            }
            serverFactory.setUserManager(userManager);

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
