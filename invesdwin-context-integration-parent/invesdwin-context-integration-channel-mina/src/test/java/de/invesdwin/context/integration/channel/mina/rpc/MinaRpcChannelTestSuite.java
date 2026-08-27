package de.invesdwin.context.integration.channel.mina.rpc;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.rpc.socket.RpcNativeSocketChannelTest;
import de.invesdwin.context.integration.channel.rpc.socket.sessionless.RpcSessionlessChannelTestSuite;

@Suite
@SelectClasses({ RpcNativeSocketChannelTest.class, RpcMinaSocketHandlerTest.class, RpcMinaDatagramHandlerTest.class,
        RpcSessionlessChannelTestSuite.class })
@Immutable
public class MinaRpcChannelTestSuite {

}
