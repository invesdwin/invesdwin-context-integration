package de.invesdwin.context.integration.channel.sync.socket;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.sync.socket.tcp.BidiSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.BidiSocketDtlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.BidiSocketTlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BidiBlockingSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.TlsBidiBlockingSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.TlsBlockingSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.BidiNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.BidiNativeSocketDtlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.BidiNativeSocketTlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramDtlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramTlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramDtlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramTlsHandshakeProviderTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ SocketChannelTest.class, BidiSocketChannelTest.class, BlockingSocketChannelTest.class,
        BidiBlockingSocketChannelTest.class, TlsBlockingSocketChannelTest.class, TlsBidiBlockingSocketChannelTest.class,
        NativeSocketChannelTest.class, BidiNativeSocketChannelTest.class, DatagramChannelTest.class,
        BlockingDatagramChannelTest.class, NativeDatagramChannelTest.class,
        BidiNativeSocketTlsHandshakeProviderTest.class, BidiNativeSocketDtlsHandshakeProviderTest.class,
        BidiSocketTlsHandshakeProviderTest.class, BidiSocketDtlsHandshakeProviderTest.class,
        NativeDatagramTlsHandshakeProviderTest.class, NativeDatagramDtlsHandshakeProviderTest.class,
        DatagramTlsHandshakeProviderTest.class, DatagramDtlsHandshakeProviderTest.class })
@Immutable
public class SocketChannelTestSuite {
    //CHECKSTYLE:ON

}
