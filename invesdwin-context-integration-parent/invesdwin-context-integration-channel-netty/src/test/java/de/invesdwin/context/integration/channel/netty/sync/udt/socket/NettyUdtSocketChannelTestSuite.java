package de.invesdwin.context.integration.channel.netty.sync.udt.socket;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ UdtChannelTest.class, BidiUdtChannelTest.class, BidiUdtTlsHandshakeProviderTest.class,
        BidiUdtDtlsHandshakeProviderTest.class })
@Immutable
public class NettyUdtSocketChannelTestSuite {

}
