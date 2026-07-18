package de.invesdwin.context.integration.channel.netty.sync;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.netty.sync.queue.NettyQueueChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.tcp.BidiNettySocketChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.tcp.NettySocketChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.tcp.TlsBidiNettySocketChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.tcp.TlsNettySocketChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.tcp.unsafe.BidiNettyNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.tcp.unsafe.NettyNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.udp.BidiNettyDatagramChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.udp.NettyDatagramChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.udp.unsafe.NettyNativeDatagramChannelTest;
import de.invesdwin.context.integration.channel.netty.sync.udt.NettyUdtChannelTestSuite;

@Suite
@SelectClasses({ TlsNettySocketChannelTest.class, NettySocketChannelTest.class, BidiNettySocketChannelTest.class,
        TlsBidiNettySocketChannelTest.class, NettyNativeSocketChannelTest.class, BidiNettyNativeSocketChannelTest.class,
        NettyDatagramChannelTest.class, BidiNettyDatagramChannelTest.class, NettyNativeDatagramChannelTest.class,
        NettyNativeDatagramChannelTest.class, NettyQueueChannelTest.class, NettyUdtChannelTestSuite.class })
@Immutable
public class NettySyncChannelTestSuite {

}
