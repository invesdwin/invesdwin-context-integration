package de.invesdwin.context.integration.channel.rpc;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.rpc.server.sessionless.RpcSessionlessChannelTestSuite;

@Suite
@SelectClasses({ RpcNativeSocketChannelTest.class, RpcNettySocketHandlerTest.class, RpcNettyDatagramHandlerTest.class,
        RpcNettyUdtHandlerTest.class, RpcUdtChannelTest.class, RpcMinaSocketHandlerTest.class,
        RpcMinaDatagramHandlerTest.class, RpcSessionlessChannelTestSuite.class })
@Immutable
public class RpcChannelTestSuite {

}
