package de.invesdwin.context.integration.channel.sync.jucx.type;

import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorkerParams;

import de.invesdwin.context.integration.channel.sync.jucx.ErrorUcxCallback;
import de.invesdwin.context.integration.channel.sync.jucx.JucxSynchronousChannel;

public interface IJucxTransportType {

    UcpRequest establishConnectionSendNonBlocking(JucxSynchronousChannel channel, long address, int length,
            ErrorUcxCallback callback);

    UcpRequest establishConnectionRecvNonBlocking(JucxSynchronousChannel channel, long address, int length,
            ErrorUcxCallback callback);

    UcpRequest sendNonBlocking(JucxSynchronousChannel channel, long address, int length, ErrorUcxCallback callback);

    UcpRequest recvNonBlocking(JucxSynchronousChannel channel, long address, int length, ErrorUcxCallback callback);

    void configureContextParams(UcpParams params);

    void configureWorkerParams(UcpWorkerParams params);

    void configureEndpointParams(UcpEndpointParams params);

    void configureMemMapParams(UcpMemMapParams params);

    void progress(JucxSynchronousChannel channel, UcpRequest request) throws Exception;

}
