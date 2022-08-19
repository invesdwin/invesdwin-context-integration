package de.invesdwin.context.integration.channel.sync.netty;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.async.netty.tcp.NettySocketHandlerTest;
import de.invesdwin.context.integration.channel.async.netty.tcp.TlsNettySocketHandlerTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.BidiNettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.TlsBidiNettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.TlsNettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udp.unsafe.NettyNativeDatagramChannelTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ TlsNettySocketChannelTest.class, NettySocketChannelTest.class, BidiNettySocketChannelTest.class,
        TlsBidiNettySocketChannelTest.class, NettyNativeSocketChannelTest.class, NettyDatagramChannelTest.class,
        NettyNativeDatagramChannelTest.class, NettyQueueChannelTest.class, NettySocketHandlerTest.class,
        TlsNettySocketHandlerTest.class })
@Immutable
public class NettyChannelTestSuite {
    //CHECKSTYLE:ON

}
