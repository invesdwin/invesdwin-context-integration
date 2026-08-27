package de.invesdwin.context.integration.channel.jnetrobust;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ BidiJnetrobustChannelTest.class })
@Immutable
public class JnetrobustChannelTestSuite {

}
