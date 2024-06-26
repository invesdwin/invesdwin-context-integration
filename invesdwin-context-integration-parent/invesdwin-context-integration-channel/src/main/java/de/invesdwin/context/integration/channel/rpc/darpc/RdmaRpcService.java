package de.invesdwin.context.integration.channel.rpc.darpc;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerEvent;
import com.ibm.darpc.DaRPCService;

@NotThreadSafe
public class RdmaRpcService extends RdmaRpcProtocol implements DaRPCService<RdmaRpcMessage, RdmaRpcMessage> {

    private DaRPCServerEvent<RdmaRpcMessage, RdmaRpcMessage> event;

    public RdmaRpcService(final int socketSize) {
        super(socketSize);
    }

    @Override
    public void processServerEvent(final DaRPCServerEvent<RdmaRpcMessage, RdmaRpcMessage> event) throws IOException {
        this.event = event;
    }

    @Override
    public void open(final DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage> endpoint) {}

    @Override
    public void close(final DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage> endpoint) {}
}
