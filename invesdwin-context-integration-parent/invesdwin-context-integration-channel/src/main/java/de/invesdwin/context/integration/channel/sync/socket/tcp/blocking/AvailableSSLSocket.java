package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

import java.io.IOException;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

public class AvailableSSLSocket extends SSLSocket {

    @Override
    public String[] getSupportedCipherSuites() {
        return null;
    }

    @Override
    public String[] getEnabledCipherSuites() {
        return null;
    }

    @Override
    public void setEnabledCipherSuites(final String[] suites) {}

    @Override
    public String[] getSupportedProtocols() {
        return null;
    }

    @Override
    public String[] getEnabledProtocols() {
        return null;
    }

    @Override
    public void setEnabledProtocols(final String[] protocols) {}

    @Override
    public SSLSession getSession() {
        return null;
    }

    @Override
    public void addHandshakeCompletedListener(final HandshakeCompletedListener listener) {}

    @Override
    public void removeHandshakeCompletedListener(final HandshakeCompletedListener listener) {}

    @Override
    public void startHandshake() throws IOException {}

    @Override
    public void setUseClientMode(final boolean mode) {}

    @Override
    public boolean getUseClientMode() {
        return false;
    }

    @Override
    public void setNeedClientAuth(final boolean need) {}

    @Override
    public boolean getNeedClientAuth() {
        return false;
    }

    @Override
    public void setWantClientAuth(final boolean want) {}

    @Override
    public boolean getWantClientAuth() {
        return false;
    }

    @Override
    public void setEnableSessionCreation(final boolean flag) {}

    @Override
    public boolean getEnableSessionCreation() {
        return false;
    }

}
