package de.invesdwin.context.integration.channel.stream.socket.sessionless;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ StreamSessionlessNativeDatagramChannelTest.class })
@Immutable
public class StreamSessionlessChannelTestSuite {

}
