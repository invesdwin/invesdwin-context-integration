package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ TimeSeriesDBChannelLatencyTest.class, TimeSeriesDBChannelThroughputTest.class })
@Immutable
public class TimeSeriesDBChannelTestSuite {

}
