package de.invesdwin.context.integration.channel.sync.kafka;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.sync.kafka.confluent.ConfluentCommunityChannelTest;
import de.invesdwin.context.integration.channel.sync.kafka.nifi.KafkaNifiChannelTest;
import de.invesdwin.context.integration.channel.sync.kafka.redpanda.RedpandaChannelTest;
import de.invesdwin.context.integration.channel.sync.pulsar.PulsarChannelTest;

@Suite
@SelectClasses({ KafkaChannelTest.class, KafkaNifiChannelTest.class, RedpandaChannelTest.class, PulsarChannelTest.class,
        ConfluentCommunityChannelTest.class })

@Immutable
public class KafkaChannelTestSuite {

}
