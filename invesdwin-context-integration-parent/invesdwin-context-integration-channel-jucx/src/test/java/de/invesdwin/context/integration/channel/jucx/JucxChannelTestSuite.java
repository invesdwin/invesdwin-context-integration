package de.invesdwin.context.integration.channel.jucx;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ BidiJucxChannelTest.class, JucxChannelTest.class, RpcJucxChannelTest.class })
@Immutable
public class JucxChannelTestSuite {

}
