package de.invesdwin.context.integration.channel.stream;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Disabled;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.stream.socket.StreamSocketChannelTestSuite;

@Disabled("not yet working")
@Suite
@SelectClasses({ StreamSocketChannelTestSuite.class })
@Immutable
public class StreamChannelTestSuite {

}
