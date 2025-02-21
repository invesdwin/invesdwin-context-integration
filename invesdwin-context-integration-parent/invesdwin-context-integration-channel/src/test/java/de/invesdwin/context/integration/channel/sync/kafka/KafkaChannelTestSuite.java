package de.invesdwin.context.integration.channel.sync.kafka;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ KafkaChannelLatencyTest.class, KafkaChannelThroughputTest.class })
@Immutable
public class KafkaChannelTestSuite {

}
