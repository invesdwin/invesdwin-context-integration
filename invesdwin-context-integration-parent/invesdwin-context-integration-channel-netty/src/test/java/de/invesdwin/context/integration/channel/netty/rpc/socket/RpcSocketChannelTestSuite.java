package de.invesdwin.context.integration.channel.netty.rpc.socket;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ RpcNettySocketHandlerTest.class, RpcNettyDatagramHandlerTest.class, RpcNettyUdtHandlerTest.class,
        RpcUdtChannelTest.class })
@Immutable
public class RpcSocketChannelTestSuite {

}
