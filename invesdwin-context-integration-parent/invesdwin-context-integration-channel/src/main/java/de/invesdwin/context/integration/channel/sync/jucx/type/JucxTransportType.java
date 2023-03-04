package de.invesdwin.context.integration.channel.sync.jucx.type;

import javax.annotation.concurrent.Immutable;

import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorkerParams;

import de.invesdwin.context.integration.channel.sync.jucx.ErrorUcxCallback;
import de.invesdwin.context.integration.channel.sync.jucx.JucxSynchronousChannel;

@Immutable
public enum JucxTransportType implements IJucxTransportType {
    STREAM {
        @Override
        public UcpRequest establishConnectionSendNonBlocking(final JucxSynchronousChannel channel, final long address,
                final int length, final ErrorUcxCallback callback) {
            return sendNonBlocking(channel, address, length, callback);
        }

        @Override
        public UcpRequest establishConnectionRecvNonBlocking(final JucxSynchronousChannel channel, final long address,
                final int length, final ErrorUcxCallback callback) {
            return recvNonBlocking(channel, address, length, callback);
        }

        @Override
        public UcpRequest sendNonBlocking(final JucxSynchronousChannel channel, final long address, final int length,
                final ErrorUcxCallback callback) {
            return channel.getUcpEndpoint().sendStreamNonBlocking(address, length, callback);
        }

        @Override
        public UcpRequest recvNonBlocking(final JucxSynchronousChannel channel, final long address, final int length,
                final ErrorUcxCallback callback) {
            return channel.getUcpEndpoint()
                    .recvStreamNonBlocking(address, length, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, callback);
        }

        @Override
        public void configureContextParams(final UcpParams params) {
            params.requestStreamFeature();
        }

        @Override
        public void configureWorkerParams(final UcpWorkerParams params) {}

        @Override
        public void configureMemMapParams(final UcpMemMapParams params) {}

        @Override
        public void configureEndpointParams(final UcpEndpointParams params) {}

        @Override
        public void progress(final JucxSynchronousChannel channel, final UcpRequest request) throws Exception {
            //this is actually non blocking
            channel.getUcpWorker().progress();
        }
    },
    TAG {
        @Override
        public UcpRequest establishConnectionSendNonBlocking(final JucxSynchronousChannel channel, final long address,
                final int length, final ErrorUcxCallback callback) {
            return channel.getUcpEndpoint().sendTaggedNonBlocking(address, length, 0, callback);
        }

        @Override
        public UcpRequest establishConnectionRecvNonBlocking(final JucxSynchronousChannel channel, final long address,
                final int length, final ErrorUcxCallback callback) {
            return channel.getUcpWorker().recvTaggedNonBlocking(address, length, 0, 0, callback);
        }

        @Override
        public UcpRequest sendNonBlocking(final JucxSynchronousChannel channel, final long address, final int length,
                final ErrorUcxCallback callback) {
            return channel.getUcpEndpoint().sendTaggedNonBlocking(address, length, channel.getLocalTag(), callback);
        }

        @Override
        public UcpRequest recvNonBlocking(final JucxSynchronousChannel channel, final long address, final int length,
                final ErrorUcxCallback callback) {
            return channel.getUcpWorker()
                    .recvTaggedNonBlocking(address, length, channel.getRemoteTag(), JucxSynchronousChannel.TAG_MASK_ALL,
                            callback);
        }

        @Override
        public void configureContextParams(final UcpParams params) {
            params.requestTagFeature();
        }

        @Override
        public void configureWorkerParams(final UcpWorkerParams params) {}

        @Override
        public void configureMemMapParams(final UcpMemMapParams params) {}

        @Override
        public void configureEndpointParams(final UcpEndpointParams params) {}

        @Override
        public void progress(final JucxSynchronousChannel channel, final UcpRequest request) throws Exception {
            //somehow tag send/receive does not work without progressRequest blocking loop
            channel.getUcpWorker().progressRequest(request);
        }
    },
    HADRONIO {
        @Override
        public UcpRequest establishConnectionSendNonBlocking(final JucxSynchronousChannel channel, final long address,
                final int length, final ErrorUcxCallback callback) {
            return STREAM.establishConnectionSendNonBlocking(channel, address, length, callback);
        }

        @Override
        public UcpRequest establishConnectionRecvNonBlocking(final JucxSynchronousChannel channel, final long address,
                final int length, final ErrorUcxCallback callback) {
            return STREAM.establishConnectionRecvNonBlocking(channel, address, length, callback);
        }

        @Override
        public UcpRequest sendNonBlocking(final JucxSynchronousChannel channel, final long address, final int length,
                final ErrorUcxCallback callback) {
            return TAG.sendNonBlocking(channel, address, length, callback);
        }

        @Override
        public UcpRequest recvNonBlocking(final JucxSynchronousChannel channel, final long address, final int length,
                final ErrorUcxCallback callback) {
            return TAG.recvNonBlocking(channel, address, length, callback);
        }

        @Override
        public void configureContextParams(final UcpParams params) {
            params.requestWakeupFeature().requestTagFeature().requestStreamFeature().setMtWorkersShared(true);
        }

        @Override
        public void configureWorkerParams(final UcpWorkerParams params) {
            params.requestWakeupTagSend().requestWakeupTagRecv();
        }

        @Override
        public void configureMemMapParams(final UcpMemMapParams params) {}

        @Override
        public void configureEndpointParams(final UcpEndpointParams params) {
            params.setPeerErrorHandlingMode();
        }

        @Override
        public void progress(final JucxSynchronousChannel channel, final UcpRequest request) throws Exception {
            TAG.progress(channel, request);
        }
    };

    public static final JucxTransportType DEFAULT = STREAM;

    @Override
    public boolean shouldCloseUcpListenerAfterAccept() {
        //Soft-RoCe does not work when closing listener after accepting a client endpoint, the close just hangs
        return false;
    }

}
