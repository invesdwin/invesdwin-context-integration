package de.invesdwin.context.integration.channel.jnanomsg;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ JnanomsgChannelTest.class })
@Immutable
public class JnanomsgChannelTestSuite {

}
