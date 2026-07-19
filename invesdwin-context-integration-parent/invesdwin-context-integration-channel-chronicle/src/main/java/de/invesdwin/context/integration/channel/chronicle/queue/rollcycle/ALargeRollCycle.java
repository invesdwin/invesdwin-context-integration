package de.invesdwin.context.integration.channel.chronicle.queue.rollcycle;

import javax.annotation.concurrent.Immutable;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.RollCycleArithmetic;

@Immutable
public abstract class ALargeRollCycle implements RollCycle {

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;
    private final long maxMessagesPerCycle;

    protected ALargeRollCycle(final String format, final int lengthInMillis) {
        this.format = format;
        this.lengthInMillis = lengthInMillis;
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