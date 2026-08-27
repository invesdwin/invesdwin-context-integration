package de.invesdwin.context.integration.channel.netty.async;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.netty.async.tcp.NettySocketHandlerTest;
import de.invesdwin.context.integration.channel.netty.async.tcp.TlsNettySocketHandlerTest;
import de.invesdwin.context.integration.channel.netty.async.udp.NettyDatagramHandlerTest;
import de.invesdwin.context.integration.channel.netty.async.udt.NettyUdtHandlerTest;

@Suite
@SelectClasses({ NettySocketHandlerTest.class, TlsNettySocketHandlerTest.class, NettyDatagramHandlerTest.class,
        NettyUdtHandlerTest.class })
@Immutable
public class NettyAsyncChannelTestSuite {

}
