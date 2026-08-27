package de.invesdwin.context.integration.channel.chronicle.queue.rollcycle;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.time.date.FTimeUnit;

/**
 * A custom roll cycle combining a 1-minute rotation with the massive sequence capacity of LARGE_DAILY. Useful for
 * throughput testing.
 */
@Immutable
public final class LargeOneMinutelyRollCycle extends ALargeRollCycle {

    public static final LargeOneMinutelyRollCycle INSTANCE = new LargeOneMinutelyRollCycle();

    private LargeOneMinutelyRollCycle() {
        // Appending '1L' prevents file format collisions with other roll cycles
        super("yyyyMMdd-HHmm'1L'", FTimeUnit.SECONDS_IN_MINUTE * FTimeUnit.MILLISECONDS_IN_SECOND);
    }

}