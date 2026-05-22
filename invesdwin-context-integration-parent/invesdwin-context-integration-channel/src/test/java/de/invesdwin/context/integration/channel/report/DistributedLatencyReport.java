package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.clock.IFDateClock;

@NotThreadSafe
public class DistributedLatencyReport extends ALatencyReport {

    private final IFDateClock clock;

    public DistributedLatencyReport(final String name) {
        this(name, FDates.getDefaultClock());
    }

    public DistributedLatencyReport(final String name, final IFDateClock clock) {
        super(name, clock.getPrecision());
        this.clock = clock;
    }

    @Override
    protected FDate newTimestamp() {
        return clock.now();
    }

}
