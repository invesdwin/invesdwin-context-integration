package de.invesdwin.context.integration.channel.sync.hadronio;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.sync.hadronio.blocking.BidiBlockingHadronioSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.hadronio.blocking.BlockingHadronioSocketChannelTest;

@Suite
@SelectClasses({ BidiBlockingHadronioSocketChannelTest.class, BlockingHadronioSocketChannelTest.class,
        BidiHadronioSocketChannelTest.class, HadronioSocketChannelTest.class })
@Immutable
public class HadronioChannelTestSuite {

}
