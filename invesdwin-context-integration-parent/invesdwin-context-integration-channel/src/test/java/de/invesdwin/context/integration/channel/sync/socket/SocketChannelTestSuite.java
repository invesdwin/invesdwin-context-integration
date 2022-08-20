package de.invesdwin.context.integration.channel.sync.socket;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.sync.socket.tcp.BidiSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BidiBlockingSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.BidiNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramChannelTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ SocketChannelTest.class, BidiSocketChannelTest.class, BlockingSocketChannelTest.class,
        BidiBlockingSocketChannelTest.class, NativeSocketChannelTest.class, BidiNativeSocketChannelTest.class,
        DatagramChannelTest.class, BlockingDatagramChannelTest.class, NativeDatagramChannelTest.class })
@Immutable
public class SocketChannelTestSuite {
    //CHECKSTYLE:ON

}
