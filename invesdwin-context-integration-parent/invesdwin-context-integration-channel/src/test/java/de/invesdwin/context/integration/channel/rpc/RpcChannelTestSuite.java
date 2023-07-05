package de.invesdwin.context.integration.channel.rpc;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.rpc.server.sessionless.RpcSessionlessNativeDatagramChannelTest;

@Suite
@SelectClasses({ RpcNativeSocketChannelTest.class, RpcNettySocketHandlerTest.class, RpcNettyDatagramHandlerTest.class,
        RpcNettyUdtHandlerTest.class, RpcMinaSocketHandlerTest.class, RpcMinaDatagramHandlerTest.class,
        RpcSessionlessNativeDatagramChannelTest.class })
@Immutable
public class RpcChannelTestSuite {

}
