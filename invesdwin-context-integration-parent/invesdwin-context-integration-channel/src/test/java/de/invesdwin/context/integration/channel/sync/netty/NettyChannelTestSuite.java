package de.invesdwin.context.integration.channel.sync.netty;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.async.netty.tcp.NettySocketHandlerTest;
import de.invesdwin.context.integration.channel.async.netty.tcp.TlsNettySocketHandlerTest;
import de.invesdwin.context.integration.channel.async.netty.udp.NettyDatagramHandlerTest;
import de.invesdwin.context.integration.channel.async.netty.udt.NettyUdtHandlerTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.BidiNettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.TlsBidiNettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.TlsNettySocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.BidiNettyNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udp.BidiNettyDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udp.unsafe.NettyNativeDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udt.BidiNettyUdtSynchronousChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udt.NettyUdtSynchronousChannelTest;

@Suite
@SelectClasses({ TlsNettySocketChannelTest.class, NettySocketChannelTest.class, BidiNettySocketChannelTest.class,
        TlsBidiNettySocketChannelTest.class, NettyNativeSocketChannelTest.class, BidiNettyNativeSocketChannelTest.class,
        NettyDatagramChannelTest.class, BidiNettyDatagramChannelTest.class, NettyNativeDatagramChannelTest.class,
        NettyNativeDatagramChannelTest.class, NettyQueueChannelTest.class, NettySocketHandlerTest.class,
        TlsNettySocketHandlerTest.class, NettyDatagramHandlerTest.class, NettyUdtHandlerTest.class,
        NettyUdtSynchronousChannelTest.class, BidiNettyUdtSynchronousChannelTest.class })
@Immutable
public class NettyChannelTestSuite {

}
