package de.invesdwin.context.integration.channel.jeromq;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ JeromqChannelTest.class })
@Immutable
public class JeromqChannelTestSuite {

}
