package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ ConfluentServerChannelTest.class, SchemaRegistryTest.class })
@Immutable
public class ConfluentChannelTestSuite {

}
