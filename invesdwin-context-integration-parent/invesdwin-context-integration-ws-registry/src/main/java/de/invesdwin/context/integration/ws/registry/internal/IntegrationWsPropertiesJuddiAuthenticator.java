package de.invesdwin.context.integration.ws.registry.internal;

import javax.annotation.concurrent.Immutable;

import org.apache.juddi.v3.auth.JUDDIAuthenticator;
import org.apache.juddi.v3.error.AuthenticationException;
import org.apache.juddi.v3.error.ErrorMessage;
import org.apache.juddi.v3.error.UnknownUserException;

import de.invesdwin.context.integration.ws.IntegrationWsProperties;

/**
 * Authentification via WebserviceProperties. So that login data must not be maintained elsewhere.
 * 
 * @author subes
 * 
 */
@Immutable
public class IntegrationWsPropertiesJuddiAuthenticator extends JUDDIAuthenticator {

    @Override
    public String authenticate(final String userID, final String credential) throws AuthenticationException {
        // a userID must be specified.
        if (userID == null) {
            throw new UnknownUserException(new ErrorMessage("errors.auth.InvalidUserId", null));
        }

        // credential (password) must be specified.
        if (credential == null) {
            throw new UnknownUserException(new ErrorMessage("errors.auth.InvalidCredentials"));
        }

        if (IntegrationWsProperties.REGISTRY_SERVER_USER.equals(userID)) {
            if (!IntegrationWsProperties.REGISTRY_SERVER_PASSWORD.equals(credential)) {
                throw new UnknownUserException(new ErrorMessage("errors.auth.InvalidCredentials"));
            }
        } else {
            throw new UnknownUserException(new ErrorMessage("errors.auth.InvalidUserId", userID));
        }

        return super.authenticate(userID, credential);
    }

}
