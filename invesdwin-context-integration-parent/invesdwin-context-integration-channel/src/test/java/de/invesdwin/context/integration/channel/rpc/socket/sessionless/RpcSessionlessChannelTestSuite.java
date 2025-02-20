package de.invesdwin.context.integration.channel.rpc.socket.sessionless;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ RpcSessionlessBlockingDatagramChannelTest.class, RpcSessionlessDatagramChannelTest.class,
        RpcSessionlessNativeDatagramChannelTest.class })
@Immutable
public class RpcSessionlessChannelTestSuite {

}
