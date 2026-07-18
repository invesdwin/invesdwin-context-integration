package de.invesdwin.context.integration.channel.netty.rpc;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.netty.rpc.socket.RpcSocketChannelTestSuite;

@Suite
@SelectClasses({ RpcSocketChannelTestSuite.class })
@Immutable
public class RpcChannelTestSuite {

}
