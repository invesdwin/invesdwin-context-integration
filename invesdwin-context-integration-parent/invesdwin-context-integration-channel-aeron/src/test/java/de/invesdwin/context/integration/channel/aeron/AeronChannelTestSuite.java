package de.invesdwin.context.integration.channel.aeron;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.aeron.agrona.AgronaChannelTest;

@Suite
@SelectClasses({ AeronChannelTest.class, AgronaChannelTest.class })

@Immutable
public class AeronChannelTestSuite {

}
