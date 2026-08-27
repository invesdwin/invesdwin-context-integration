package de.invesdwin.context.integration.ws.internal;

import javax.annotation.concurrent.Immutable;

import org.springframework.security.core.userdetails.User;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

import de.invesdwin.context.integration.ws.IntegrationWsProperties;

@Immutable
public class SpringWebUserDetailsManager extends InMemoryUserDetailsManager {

    public SpringWebUserDetailsManager() {
        super(User.withUsername(IntegrationWsProperties.SPRING_WEB_USER)
                .password(IntegrationWsProperties.SPRING_WEB_PASSWORD)
                .authorities("_SPRING_WEB_")
                .build());
    }
}