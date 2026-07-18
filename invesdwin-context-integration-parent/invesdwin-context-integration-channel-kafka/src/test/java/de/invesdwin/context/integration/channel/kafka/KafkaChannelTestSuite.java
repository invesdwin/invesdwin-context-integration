package de.invesdwin.context.integration.channel.kafka;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.kafka.confluent.ConfluentCommunityChannelTest;
import de.invesdwin.context.integration.channel.kafka.nifi.KafkaNifiChannelTest;
import de.invesdwin.context.integration.channel.kafka.redpanda.RedpandaChannelTest;

@Suite
@SelectClasses({ KafkaChannelTest.class, KafkaNifiChannelTest.class, RedpandaChannelTest.class,
        ConfluentCommunityChannelTest.class })

@Immutable
public class KafkaChannelTestSuite {

}
