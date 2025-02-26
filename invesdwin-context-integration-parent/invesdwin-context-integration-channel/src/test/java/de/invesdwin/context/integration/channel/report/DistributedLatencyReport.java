package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.time.date.FTimeUnit;

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
    protected FTimeUnit newMeasureTimeUnit() {
        return FTimeUnit.MILLISECONDS;
    }

}
