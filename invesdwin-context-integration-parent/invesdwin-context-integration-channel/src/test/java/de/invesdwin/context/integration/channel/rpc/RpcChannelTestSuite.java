package de.invesdwin.context.integration.channel.rpc;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ RpcNativeSocketChannelTest.class, RpcNettySocketHandlerTest.class, RpcNettyUdtHandlerTest.class,
        RpcMinaSocketHandlerTest.class, RpcMinaDatagramHandlerTest.class })
@Immutable
public class RpcChannelTestSuite {

}
