package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class LocalNanosLatencyReportFactory implements ILatencyReportFactory {

    public static final LocalNanosLatencyReportFactory INSTANCE = new LocalNanosLatencyReportFactory();

    private LocalNanosLatencyReportFactory() {}

    @Override
    public ILatencyReport newLatencyReport(final String name) {
        return new LocalLatencyReport(name);
    }

    @Override
    public boolean isMeasuringLatency() {
        return true;
    }

}
