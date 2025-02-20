package de.invesdwin.context.integration.channel.stream.socket;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.stream.socket.sessionless.StreamSessionlessChannelTestSuite;

@Suite
@SelectClasses({ StreamNativeSocketChannelTest.class, StreamNettyDatagramHandlerTest.class,
        StreamNettySocketHandlerTest.class, StreamSessionlessChannelTestSuite.class })

@Immutable
public class StreamSocketChannelTestSuite {

}
