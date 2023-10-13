package de.invesdwin.context.integration.channel.rpc.rmi;

import java.io.IOException;
import java.rmi.RemoteException;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.remoting.rmi.rmi.RmiServiceExporter;

import de.invesdwin.context.integration.channel.rpc.rmi.service.IRmiSynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.rmi.service.RmiSynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.blocking.ABlockingSynchronousEndpointServer;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.lang.Closeables;

@SuppressWarnings("deprecation")
@NotThreadSafe
public class RmiSynchronousEndpointServer extends ABlockingSynchronousEndpointServer {

    public static final String DEFAULT_SERVICE_NAME = RmiSynchronousEndpointServer.class.getSimpleName();
    public static final int DEFAULT_REGISTRY_PORT = 1099;

    private final String serviceName;
    private final int registryPort;
    private RmiServiceExporter server;

    public RmiSynchronousEndpointServer(final AsynchronousEndpointServerHandlerFactory handlerFactory) {
        super(handlerFactory);
        this.serviceName = newServiceName();
        this.registryPort = newRegistryPort();
    }

    protected String newServiceName() {
        return DEFAULT_SERVICE_NAME;
    }

    protected int newRegistryPort() {
        return DEFAULT_REGISTRY_PORT;
    }

    public String getServiceName() {
        return serviceName;
    }

    public int getRegistryPort() {
        return registryPort;
    }

    @Override
    public void open() throws IOException {
        if (server != null) {
            throw new IllegalStateException("server should be null");
        }
        server = new RmiServiceExporter();
        //<property name="serviceName" value="HelloService"/>
        server.setServiceName(serviceName);
        //<property name="service" ref="helloService"/>
        server.setService(new RmiSynchronousEndpointService(this));
        //<property name="serviceInterface" value="com.etymgiko.springrmiexample.shared.HelloService"/>
        server.setServiceInterface(IRmiSynchronousEndpointService.class);
        //<!-- defaults to 1099 -->
        //<property name="registryPort" value="1099"/>
        server.setRegistryPort(registryPort);
        //</bean>
        server.afterPropertiesSet();
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            Closeables.closeQuietly(server.getService());
            try {
                server.destroy();
            } catch (final RemoteException e) {
                Err.process(new RuntimeException("Ignoring", e));
            }
            server = null;
        }
    }

}
