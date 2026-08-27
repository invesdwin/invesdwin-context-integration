package de.invesdwin.context.integration.channel.mina.sync;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.mina.sync.apr.MinaNativeDatagramChannelTest;
import de.invesdwin.context.integration.channel.mina.sync.unsafe.BidiMinaNativeSocketChannelTest;

@Suite
@SelectClasses({ MinaSocketChannelTest.class, BidiMinaSocketChannelTest.class, TlsBidiMinaSocketChannelTest.class,
        BidiMinaNativeSocketChannelTest.class, MinaNativeDatagramChannelTest.class })
@Immutable
public class MinaSyncChannelTestSuite {

}
