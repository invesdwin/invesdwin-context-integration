package de.invesdwin.context.integration.channel.rpc.rdma;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ RpcJucxChannelTest.class })
@Immutable
public class RpcRdmaChannelTestSuite {

}
