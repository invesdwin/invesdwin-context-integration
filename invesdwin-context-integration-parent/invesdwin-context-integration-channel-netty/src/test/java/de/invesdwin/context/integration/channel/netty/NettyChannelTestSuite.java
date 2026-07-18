package de.invesdwin.context.integration.channel.netty;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.netty.async.NettyAsyncChannelTestSuite;
import de.invesdwin.context.integration.channel.netty.rpc.RpcChannelTestSuite;
import de.invesdwin.context.integration.channel.netty.stream.NettyStreamChannelTestSuite;
import de.invesdwin.context.integration.channel.netty.sync.NettySyncChannelTestSuite;

@Suite
@SelectClasses({ NettyAsyncChannelTestSuite.class, NettySyncChannelTestSuite.class, RpcChannelTestSuite.class,
        NettyStreamChannelTestSuite.class })
@Immutable
public class NettyChannelTestSuite {

}
