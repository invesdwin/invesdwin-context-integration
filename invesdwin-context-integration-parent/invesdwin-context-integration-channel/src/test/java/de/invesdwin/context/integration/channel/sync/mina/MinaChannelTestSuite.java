package de.invesdwin.context.integration.channel.sync.mina;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.async.mina.MinaSocketHandlerTest;
import de.invesdwin.context.integration.channel.async.mina.TlsMinaSocketHandlerTest;
import de.invesdwin.context.integration.channel.sync.mina.apr.MinaNativeDatagramChannelTest;
import de.invesdwin.context.integration.channel.sync.mina.unsafe.BidiMinaNativeSocketChannelTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ MinaSocketHandlerTest.class, MinaSocketChannelTest.class, BidiMinaSocketChannelTest.class,
        TlsMinaSocketHandlerTest.class, TlsBidiMinaSocketChannelTest.class, BidiMinaNativeSocketChannelTest.class,
        MinaNativeDatagramChannelTest.class })
@Immutable
public class MinaChannelTestSuite {
    //CHECKSTYLE:ON

}
