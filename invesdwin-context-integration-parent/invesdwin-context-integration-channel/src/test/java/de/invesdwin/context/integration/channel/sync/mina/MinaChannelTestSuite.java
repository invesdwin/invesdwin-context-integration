package de.invesdwin.context.integration.channel.sync.mina;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.async.mina.MinaSocketHandlerTest;
import de.invesdwin.context.integration.channel.async.mina.TlsMinaSocketHandlerTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ MinaSocketHandlerTest.class, MinaSocketChannelTest.class, BidiMinaSocketChannelTest.class,
        TlsMinaSocketHandlerTest.class, TlsMinaSocketChannelTest.class, TlsBidiMinaSocketChannelTest.class })
@Immutable
public class MinaChannelTestSuite {
    //CHECKSTYLE:ON

}
