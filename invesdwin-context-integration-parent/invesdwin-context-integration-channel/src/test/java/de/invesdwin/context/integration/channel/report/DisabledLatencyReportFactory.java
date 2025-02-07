package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class DisabledLatencyReportFactory implements ILatencyReportFactory {

    public static final DisabledLatencyReportFactory INSTANCE = new DisabledLatencyReportFactory();

    private DisabledLatencyReportFactory() {}

    @Override
    public ILatencyReport newLatencyReport(final String name) {
        return new DisabledLatencyReport();
    }

    @Override
    public boolean isMeasuringLatency() {
        return false;
    }

}
