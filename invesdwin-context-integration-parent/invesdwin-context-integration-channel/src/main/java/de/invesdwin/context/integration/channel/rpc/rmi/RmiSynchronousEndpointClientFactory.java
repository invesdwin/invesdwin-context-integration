package de.invesdwin.context.integration.channel.rpc.rmi;

import javax.annotation.concurrent.Immutable;

import org.springframework.remoting.rmi.rmi.RmiProxyFactoryBean;

import de.invesdwin.context.integration.channel.rpc.client.blocking.ClientSideBlockingEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.server.service.blocking.BufferBlockingEndpointServiceFromArray;
import de.invesdwin.context.integration.channel.rpc.server.service.blocking.IArrayBlockingEndpointService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@SuppressWarnings("deprecation")
@Immutable
public class RmiSynchronousEndpointClientFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {

    private final String serviceUrl;
    private IArrayBlockingEndpointService service;

    public RmiSynchronousEndpointClientFactory() {
        this.serviceUrl = newServiceUrl();
    }

    protected IArrayBlockingEndpointService newService() {
        final RmiProxyFactoryBean client = new RmiProxyFactoryBean();
        client.setServiceUrl(serviceUrl);
        client.setServiceInterface(IArrayBlockingEndpointService.class);
        client.afterPropertiesSet();
        return (IArrayBlockingEndpointService) client.getObject();
    }

    protected String newServiceUrl() {
        return "//127.0.0.1:" + RmiSynchronousEndpointServer.DEFAULT_REGISTRY_PORT + "/"
                + RmiSynchronousEndpointServer.DEFAULT_SERVICE_NAME;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    private IArrayBlockingEndpointService getService() {
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
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final BufferBlockingEndpointServiceFromArray bufferService = new BufferBlockingEndpointServiceFromArray(
                getService());
        return new ClientSideBlockingEndpoint(bufferService);
    }

    @Override
    public void close() {
        service = null;
    }

}
