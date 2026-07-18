package de.invesdwin.context.integration.channel.mina;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.mina.async.MinaAsyncChannelTestSuite;
import de.invesdwin.context.integration.channel.mina.rpc.MinaRpcChannelTestSuite;
import de.invesdwin.context.integration.channel.mina.sync.MinaSyncChannelTestSuite;

@Suite
@SelectClasses({ MinaAsyncChannelTestSuite.class, MinaSyncChannelTestSuite.class, MinaRpcChannelTestSuite.class })
@Immutable
public class MinaChannelTestSuite {

}
