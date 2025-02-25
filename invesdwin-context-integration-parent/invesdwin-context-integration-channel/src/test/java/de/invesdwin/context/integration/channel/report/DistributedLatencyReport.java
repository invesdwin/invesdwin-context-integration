package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class DistributedLatencyReport extends ALatencyReport {

    public DistributedLatencyReport(final String name) {
        super(name);
    }

    @Override
    protected long newTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    protected String newLatencyHeader() {
        return "ms";
    }

}
