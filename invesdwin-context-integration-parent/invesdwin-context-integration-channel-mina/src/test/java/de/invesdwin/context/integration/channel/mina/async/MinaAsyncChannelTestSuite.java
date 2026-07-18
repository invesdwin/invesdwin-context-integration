package de.invesdwin.context.integration.channel.mina.async;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ MinaSocketHandlerTest.class, TlsMinaSocketHandlerTest.class })
@Immutable
public class MinaAsyncChannelTestSuite {

}
