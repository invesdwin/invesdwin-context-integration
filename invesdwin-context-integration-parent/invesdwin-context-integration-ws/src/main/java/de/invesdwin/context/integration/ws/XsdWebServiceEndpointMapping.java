package de.invesdwin.context.integration.ws;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.ws.context.MessageContext;
import org.springframework.ws.server.endpoint.mapping.AbstractMapBasedEndpointMapping;
import org.springframework.ws.transport.WebServiceConnection;
import org.springframework.ws.transport.context.TransportContext;
import org.springframework.ws.transport.context.TransportContextHolder;

import de.invesdwin.context.integration.marshaller.IMergedJaxbContextPath;
import de.invesdwin.util.lang.string.Strings;
import jakarta.inject.Inject;

@ThreadSafe
public class XsdWebServiceEndpointMapping extends AbstractMapBasedEndpointMapping {

    @Inject
    private IMergedJaxbContextPath[] jaxbContextPaths;

    @Override
    protected boolean validateLookupKey(final String key) {
        for (final IMergedJaxbContextPath path : jaxbContextPaths) {
            if (Strings.removeEnd(path.getSchemaPath(), ".xsd").equals("/META-INF/xsd/" + key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected String getLookupKeyForMessage(final MessageContext messageContext) throws Exception {
        final TransportContext transportContext = TransportContextHolder.getTransportContext();
        if (transportContext != null) {
            final WebServiceConnection connection = transportContext.getConnection();
            if (connection != null) {
                String lookupKey = Strings.substringAfterLast(connection.getUri().getPath(), "/");
                lookupKey = Strings.removeEnd(lookupKey, ".wsdl");
                return lookupKey;
            }
        }
        return null;
    }

}
