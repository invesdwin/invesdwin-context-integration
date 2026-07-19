package de.invesdwin.context.integration.channel.chronicle.queue.rollcycle;

import javax.annotation.concurrent.Immutable;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.RollCycleArithmetic;

/**
 * A custom roll cycle combining the 5-minute rotation of FIVE_MINUTELY with the massive sequence capacity of
 * LARGE_DAILY. Useful for throughput testing.
 */
@Immutable
public final class LargeFiveMinutelyRollCycle implements RollCycle {

    public static final LargeFiveMinutelyRollCycle INSTANCE = new LargeFiveMinutelyRollCycle();

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;
    private final long maxMessagesPerCycle;

    private LargeFiveMinutelyRollCycle() {
        // Appending 'L' prevents file format collisions with the standard FIVE_MINUTELY
        this.format = "yyyyMMdd-HHmm'L'";

        // 5 minutes in milliseconds[cite: 15]
        this.lengthInMillis = 5 * 60 * 1000;

        // Use Chronicle's internal arithmetic utility, mimicking LARGE_DAILY
        // with 32768 (MAX_INDEX_COUNT) and 128 spacing
        this.arithmetic = RollCycleArithmetic.of(32768, 128);
        this.maxMessagesPerCycle = arithmetic.maxMessagesPerCycle();
    }

    @Override
    public long maxMessagesPerCycle() {
        return this.maxMessagesPerCycle;
    }

    @Override
    public String format() {
        return this.format;
    }

    @Override
    public int lengthInMillis() {
        return this.lengthInMillis;
    }

    @Override
    public int defaultIndexCount() {
        return arithmetic.indexCount();
    }

    @Override
    public int defaultIndexSpacing() {
        return arithmetic.indexSpacing();
    }

    @Override
    public long toIndex(final int cycle, final long sequenceNumber) {
        return arithmetic.toIndex(cycle, sequenceNumber);
    }

    @Override
    public long toSequenceNumber(final long index) {
        return arithmetic.toSequenceNumber(index);
    }

    @Override
    public int toCycle(final long index) {
        return arithmetic.toCycle(index);
    }
}