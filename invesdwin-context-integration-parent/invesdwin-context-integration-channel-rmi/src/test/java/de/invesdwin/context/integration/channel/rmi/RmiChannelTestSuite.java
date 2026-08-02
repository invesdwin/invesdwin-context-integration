package de.invesdwin.context.integration.channel.rmi;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ RpcRmiChannelTest.class })
@Immutable
public class RmiChannelTestSuite {

}
