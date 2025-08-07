package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ ConfluentCommercialChannelTest.class })
@Immutable
public class ConfluentChannelTestSuite {

}
