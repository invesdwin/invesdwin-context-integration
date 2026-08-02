package de.invesdwin.context.integration.grid.jppf.admin.web;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.wicket.request.Request;
import org.jppf.admin.web.JPPFWebSession;
import org.jppf.admin.web.auth.JPPFRoles;

import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.Collections;

@NotThreadSafe
public class ConfiguredJPPFWebSession extends JPPFWebSession {

    // Grant full administrative and user roles required by JPPF Web Console
    private static final String[] ROLES = new String[] {
            org.apache.wicket.authroles.authorization.strategies.role.Roles.ADMIN,
            org.apache.wicket.authroles.authorization.strategies.role.Roles.USER, JPPFRoles.ADMIN, JPPFRoles.MANAGER,
            JPPFRoles.MONITOR };

    public ConfiguredJPPFWebSession(final Request request) {
        super(request);
    }

    @Override
    protected boolean superAuthenticate(final String username, final String password) {
        if (!IntegrationWsProperties.SPRING_WEB_USER.equals(username)) {
            return false;
        }
        if (!IntegrationWsProperties.SPRING_WEB_PASSWORD.equals(password)) {
            return false;
        }
        return true;
    }

    @Override
    public org.apache.wicket.authroles.authorization.strategies.role.Roles getRoles() {
        if (isSignedIn()) {
            return new org.apache.wicket.authroles.authorization.strategies.role.Roles(ROLES);
        }
        return null;
    }

    @Override
    protected List<String> getUserRoles() {
        if (isSignedIn()) {
            return Arrays.asList(ROLES);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public String getSignedInUser() {
        if (isSignedIn()) {
            return IntegrationWsProperties.SPRING_WEB_USER;
        }
        return null;

    }

    @Override
    public String getUserName() {
        return getSignedInUser();
    }

}
