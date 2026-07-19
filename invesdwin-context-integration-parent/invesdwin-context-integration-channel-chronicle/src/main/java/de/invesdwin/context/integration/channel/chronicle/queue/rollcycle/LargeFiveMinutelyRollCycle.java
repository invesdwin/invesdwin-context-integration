package de.invesdwin.context.integration.channel.chronicle.queue.rollcycle;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.date.FTimeUnit;

/**
 * A custom roll cycle combining the 5-minute rotation of FIVE_MINUTELY with the massive sequence capacity of
 * LARGE_DAILY. Useful for throughput testing.
 */
@Immutable
public final class LargeFiveMinutelyRollCycle extends ALargeRollCycle {

    public static final LargeFiveMinutelyRollCycle INSTANCE = new LargeFiveMinutelyRollCycle();

    private LargeFiveMinutelyRollCycle() {
        // Appending 'L' prevents file format collisions with the standard FIVE_MINUTELY
        super("yyyyMMdd-HHmm'L'", 5 * FTimeUnit.SECONDS_IN_MINUTE * FTimeUnit.MILLISECONDS_IN_SECOND);
    }
}