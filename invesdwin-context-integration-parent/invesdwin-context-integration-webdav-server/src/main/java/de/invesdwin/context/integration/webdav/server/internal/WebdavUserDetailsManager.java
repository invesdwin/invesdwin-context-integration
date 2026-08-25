package de.invesdwin.context.integration.webdav.server.internal;

import javax.annotation.concurrent.Immutable;

import org.springframework.security.core.userdetails.User;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

import de.invesdwin.context.integration.webdav.WebdavClientProperties;

@Immutable
public class WebdavUserDetailsManager extends InMemoryUserDetailsManager {

    public WebdavUserDetailsManager() {
        super(User.withUsername(WebdavClientProperties.USERNAME)
                .password(WebdavClientProperties.PASSWORD)
                .authorities("_WEBDAV_")
                .build());
    }
}