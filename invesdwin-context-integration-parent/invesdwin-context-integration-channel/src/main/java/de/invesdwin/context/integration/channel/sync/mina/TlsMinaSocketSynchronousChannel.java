package de.invesdwin.context.integration.channel.sync.mina;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;
import javax.net.ssl.SSLContext;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.ssl.SslFilter;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ClientAuth;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;

@Immutable
public class TlsMinaSocketSynchronousChannel extends MinaSocketSynchronousChannel {

    public TlsMinaSocketSynchronousChannel(final IMinaSocketType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress) super.getSocketAddress();
    }

    @Override
    protected void onSession(final IoSession session) {
        final ITransportLayerSecurityProvider tlsProvider = newTransportLayerSecurityProvider();
        final SSLContext context = tlsProvider.newContext();

        final SslFilter sslHandler = new SslFilter(context);
        sslHandler.setWantClientAuth(tlsProvider.getClientAuth() == ClientAuth.WANT);
        sslHandler.setNeedClientAuth(tlsProvider.getClientAuth() == ClientAuth.NEED);
        session.getFilterChain().addFirst("ssl", sslHandler);

        super.onSession(session);
    }

    protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
        final InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        return new DerivedKeyTransportLayerSecurityProvider(inetSocketAddress, server) {
            @Override
            protected String getHostname() {
                return inetSocketAddress.getHostName();
            }
        };
    }

}
