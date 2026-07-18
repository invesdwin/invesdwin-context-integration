package de.invesdwin.context.integration.channel.netty.sync.udt;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ NettyUdtSynchronousChannelTest.class, BidiNettyUdtSynchronousChannelTest.class,
        NettyUdtChannelTestSuite.class })
@Immutable
public class NettyUdtChannelTestSuite {

}
