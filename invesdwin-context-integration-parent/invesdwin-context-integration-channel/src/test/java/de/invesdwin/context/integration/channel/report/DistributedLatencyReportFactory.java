package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class DistributedLatencyReportFactory implements ILatencyReportFactory {

    public static final DistributedLatencyReportFactory INSTANCE = new DistributedLatencyReportFactory();

    private DistributedLatencyReportFactory() {}

    @Override
    public ILatencyReport newLatencyReport(final String name) {
        return new DistributedLatencyReport(name);
    }

    @Override
    public boolean isMeasuringLatency() {
        return true;
    }

}
