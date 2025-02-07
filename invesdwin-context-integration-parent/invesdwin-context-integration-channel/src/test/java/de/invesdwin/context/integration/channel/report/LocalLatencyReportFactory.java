package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class LocalLatencyReportFactory implements ILatencyReportFactory {

    public static final LocalLatencyReportFactory INSTANCE = new LocalLatencyReportFactory();

    private LocalLatencyReportFactory() {}

    @Override
    public ILatencyReport newLatencyReport(final String name) {
        return new LocalLatencyReport(name);
    }

    @Override
    public boolean isMeasuringLatency() {
        return true;
    }

}
