package de.invesdwin.context.integration.channel.rpc;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ RpcNativeSocketChannelTest.class, RpcNettySocketHandlerTest.class, RpcMinaSocketHandlerTest.class })
@Immutable
public class RpcChannelTestSuite {

}
