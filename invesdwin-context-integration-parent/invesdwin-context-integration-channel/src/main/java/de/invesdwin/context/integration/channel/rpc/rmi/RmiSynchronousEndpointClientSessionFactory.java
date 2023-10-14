package de.invesdwin.context.integration.channel.rpc.rmi;

import javax.annotation.concurrent.Immutable;

import org.springframework.remoting.rmi.rmi.RmiProxyFactoryBean;

import de.invesdwin.context.integration.channel.rpc.client.blocking.ClientSideBlockingSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.transformer.DefaultSynchronousEndpointSessionFactoryTransformer;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.transformer.ISynchronousEndpointSessionFactoryTransformer;
import de.invesdwin.context.integration.channel.rpc.server.service.blocking.BufferBlockingSynchronousEndpointServiceFromArray;
import de.invesdwin.context.integration.channel.rpc.server.service.blocking.IArrayBlockingSynchronousEndpointService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@SuppressWarnings("deprecation")
@Immutable
public class RmiSynchronousEndpointClientSessionFactory implements ISynchronousEndpointSessionFactory {

    private final String serviceUrl;
    private final ISynchronousEndpointSessionFactory delegate;
    private IArrayBlockingSynchronousEndpointService service;

    public RmiSynchronousEndpointClientSessionFactory() {
        this.serviceUrl = newServiceUrl();
        this.delegate = newDelegate();
    }

    public ISynchronousEndpointSessionFactory newDelegate() {
        final ISynchronousEndpointSessionFactoryTransformer sesssionFactoryTransformer = newSessionFactoryTransformer();
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> endpointFactory = new ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider>() {
            @Override
            public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
                final IArrayBlockingSynchronousEndpointService arrayService = getService();
                final BufferBlockingSynchronousEndpointServiceFromArray bufferService = new BufferBlockingSynchronousEndpointServiceFromArray(
                        arrayService);
                return new ClientSideBlockingSynchronousEndpoint(bufferService);
            }
        };
        return sesssionFactoryTransformer.transform(endpointFactory);
    }

    protected IArrayBlockingSynchronousEndpointService newService() {
        final RmiProxyFactoryBean client = new RmiProxyFactoryBean();
        client.setServiceUrl(serviceUrl);
        client.setServiceInterface(IArrayBlockingSynchronousEndpointService.class);
        client.afterPropertiesSet();
        return (IArrayBlockingSynchronousEndpointService) client.getObject();
    }

    /**
     * Could add compression and pre-shared encryption/authentication with this override.
     */
    protected ISynchronousEndpointSessionFactoryTransformer newSessionFactoryTransformer() {
        return new DefaultSynchronousEndpointSessionFactoryTransformer();
    }

    protected String newServiceUrl() {
        return "//127.0.0.1:" + RmiSynchronousEndpointServer.DEFAULT_REGISTRY_PORT + "/"
                + RmiSynchronousEndpointServer.DEFAULT_SERVICE_NAME;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    private IArrayBlockingSynchronousEndpointService getService() {
        if (service == null) {
            synchronized (this) {
                if (service == null) {
                    service = newService();
                }
            }
        }
        return service;
    }

    @Override
    public ISynchronousEndpointSession newSession() {
        return delegate.newSession();
    }

    @Override
    public void close() {
        service = null;
    }

}
