package de.invesdwin.context.integration.channel.sync.jucx;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.sync.jucx.stream.BidiJucxStreamChannelTest;
import de.invesdwin.context.integration.channel.sync.jucx.stream.JucxStreamChannelTest;
import de.invesdwin.context.integration.channel.sync.jucx.tag.BidiJucxTagChannelTest;
import de.invesdwin.context.integration.channel.sync.jucx.tag.JucxTagChannelTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ BidiJucxStreamChannelTest.class, JucxStreamChannelTest.class, BidiJucxTagChannelTest.class,
        JucxTagChannelTest.class })
@Immutable
public class JucxChannelTestSuite {
    //CHECKSTYLE:ON

}
