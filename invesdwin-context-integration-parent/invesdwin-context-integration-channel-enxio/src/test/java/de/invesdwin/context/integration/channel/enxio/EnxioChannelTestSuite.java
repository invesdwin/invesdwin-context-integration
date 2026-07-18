package de.invesdwin.context.integration.channel.enxio;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ EnxioSocketChannelTest.class, BidiEnxioSocketChannelTest.class })
@Immutable
public class EnxioChannelTestSuite {

}
