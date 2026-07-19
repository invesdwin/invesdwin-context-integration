package de.invesdwin.context.integration.channel.chronicle.queue;

import javax.annotation.concurrent.Immutable;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.RollCycleArithmetic;

/**
 * A custom roll cycle combining a 1-minute rotation with the massive sequence capacity of LARGE_DAILY.
 */
@Immutable
public final class LargeOneMinutelyRollCycle implements RollCycle {

    public static final LargeOneMinutelyRollCycle INSTANCE = new LargeOneMinutelyRollCycle();

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;
    private final long maxMessagesPerCycle;

    private LargeOneMinutelyRollCycle() {
        // Appending '1L' prevents file format collisions with other roll cycles
        this.format = "yyyyMMdd-HHmm'1L'";

        // 1 minute in milliseconds
        this.lengthInMillis = 60 * 1000;

        // Use Chronicle's internal arithmetic utility, mimicking LARGE_DAILY
        // with 32768 (MAX_INDEX_COUNT) and 128 spacing[cite: 16]
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