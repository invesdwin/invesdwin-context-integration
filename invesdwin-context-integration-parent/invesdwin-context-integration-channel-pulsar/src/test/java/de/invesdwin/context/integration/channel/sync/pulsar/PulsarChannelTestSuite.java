package de.invesdwin.context.integration.channel.sync.pulsar;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ PulsarChannelTest.class })
@Immutable
public class PulsarChannelTestSuite {

}
