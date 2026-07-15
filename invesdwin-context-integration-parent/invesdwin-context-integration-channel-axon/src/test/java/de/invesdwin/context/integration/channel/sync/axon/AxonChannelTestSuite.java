package de.invesdwin.context.integration.channel.sync.axon;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ AxonChannelTest.class })
@Immutable
public class AxonChannelTestSuite {

}
