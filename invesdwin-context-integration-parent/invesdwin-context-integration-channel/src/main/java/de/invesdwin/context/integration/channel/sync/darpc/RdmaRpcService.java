package de.invesdwin.context.integration.channel.sync.darpc;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerEvent;
import com.ibm.darpc.DaRPCService;

@NotThreadSafe
public class RdmaRpcService extends RdmaRpcProtocol implements DaRPCService<RdmaRpcMessage, RdmaRpcMessage> {

    @Override
    public void processServerEvent(final DaRPCServerEvent<RdmaRpcMessage, RdmaRpcMessage> event) throws IOException {
        final RdmaRpcMessage request = event.getReceiveMessage();
        final RdmaRpcMessage response = event.getSendMessage();
        response.setName(request.getParam() + 1);
        event.triggerResponse();
    }

    @Override
    public void open(final DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage> endpoint) {
        System.out.println("new connection " + endpoint.getEndpointId() + ", cluster " + endpoint.clusterId());
    }

    @Override
    public void close(final DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage> endpoint) {
        System.out.println("disconnecting " + endpoint.getEndpointId() + ", cluster " + endpoint.clusterId());
    }
}
