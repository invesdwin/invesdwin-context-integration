package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class ValidatingLatencyReportFactory implements ILatencyReportFactory {

    public static final ValidatingLatencyReportFactory INSTANCE = new ValidatingLatencyReportFactory();

    private ValidatingLatencyReportFactory() {}

    @Override
    public ILatencyReport newLatencyReport(final String name) {
        return new ValidatingLatencyReport();
    }

    @Override
    public boolean isMeasuringLatency() {
        return false;
    }

}
