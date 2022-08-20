package de.invesdwin.context.integration.channel.sync.chronicle;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.sync.chronicle.network.BidiChronicleNetworkChannelTest;
import de.invesdwin.context.integration.channel.sync.chronicle.network.ChronicleNetworkChannelTest;
import de.invesdwin.context.integration.channel.sync.chronicle.queue.ChronicleQueueChannelTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ ChronicleNetworkChannelTest.class, BidiChronicleNetworkChannelTest.class,
        ChronicleQueueChannelTest.class })
@Immutable
public class ChronicleChannelTestSuite {
    //CHECKSTYLE:ON

}
