package de.invesdwin.context.integration.channel.chronicle;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.chronicle.network.BidiChronicleNetworkChannelTest;
import de.invesdwin.context.integration.channel.chronicle.network.ChronicleNetworkChannelTest;
import de.invesdwin.context.integration.channel.chronicle.queue.ChronicleQueueChannelTest;

@Suite
@SelectClasses({ ChronicleNetworkChannelTest.class, BidiChronicleNetworkChannelTest.class,
        ChronicleQueueChannelTest.class })
@Immutable
public class ChronicleChannelTestSuite {

}
