package de.invesdwin.context.integration.channel.report;

public interface ILatencyReportFactory {

    ILatencyReport newLatencyReport(String name);

    boolean isMeasuringLatency();

}
