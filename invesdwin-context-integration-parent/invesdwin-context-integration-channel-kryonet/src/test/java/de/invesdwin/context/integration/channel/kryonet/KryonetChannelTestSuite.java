package de.invesdwin.context.integration.channel.kryonet;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ KryonetChannelTest.class })
@Immutable
public class KryonetChannelTestSuite {

}
