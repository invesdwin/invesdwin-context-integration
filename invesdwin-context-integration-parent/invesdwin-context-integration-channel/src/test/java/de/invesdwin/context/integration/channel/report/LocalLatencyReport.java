package de.invesdwin.context.integration.channel.report;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.time.date.FTimeUnit;

/**
 * This report uses nano precision based on cpu clock in the JVM. Using this measure between processes will not work as
 * each JVM will have its own cpu clock. Use DistributedLatencyReport instead for multi-process or multi-machine setups.
 */
@NotThreadSafe
public class LocalLatencyReport extends ALatencyReport {

    public LocalLatencyReport(final String name) {
        super(name);
    }

    @Override
    protected long newTimestamp() {
        return System.nanoTime();
    }

    @Override
    protected FTimeUnit newMeasureTimeUnit() {
        return FTimeUnit.NANOSECONDS;
    }

}
