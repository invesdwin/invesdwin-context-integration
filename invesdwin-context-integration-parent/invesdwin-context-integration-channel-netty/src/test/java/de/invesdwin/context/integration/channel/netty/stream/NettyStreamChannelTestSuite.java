package de.invesdwin.context.integration.channel.netty.stream;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ StreamNettyDatagramHandlerTest.class, StreamNettySocketHandlerTest.class })

@Immutable
public class NettyStreamChannelTestSuite {

}
