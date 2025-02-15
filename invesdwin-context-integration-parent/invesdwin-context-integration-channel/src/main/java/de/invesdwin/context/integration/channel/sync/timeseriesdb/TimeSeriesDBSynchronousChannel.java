package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.compression.lz4.LZ4Streams;
import de.invesdwin.context.persistence.timeseriesdb.segmented.PeriodicalSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.DummySegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.timed.IndexedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.timed.IndexedByteBufferSerde;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class TimeSeriesDBSynchronousChannel implements ISynchronousChannel {

    private final File directory;
    private ALiveSegmentedTimeSeriesDB<String, IndexedByteBuffer> database;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private final String hashKey;
    private final Integer valueFixedLength;
    private final TimeSeriesDBSynchronousChannelIndexMode indexMode;
    private final boolean closeMessageEnabled;

    public TimeSeriesDBSynchronousChannel(final File directory, final Integer valueFixedLength) {
        this.directory = directory;
        this.valueFixedLength = valueFixedLength;
        this.hashKey = newHashKey();
        this.indexMode = newIndexMode();
        this.closeMessageEnabled = newCloseMessageEnabled();
    }

    protected boolean newCloseMessageEnabled() {
        return true;
    }

    protected String newHashKey() {
        return "";
    }

    protected TimeSeriesDBSynchronousChannelIndexMode newIndexMode() {
        return TimeSeriesDBSynchronousChannelIndexMode.INDEX;
    }

    public TimeSeriesDBSynchronousChannelIndexMode getIndexMode() {
        return indexMode;
    }

    public ICompressionFactory newCompressionFactory() {
        return LZ4Streams.getDefaultCompressionFactory();
    }

    public boolean isCloseMessageEnabled() {
        return closeMessageEnabled;
    }

    public Integer getValueFixedLength() {
        return valueFixedLength;
    }

    @Override
    public synchronized void open() throws IOException {
        if (!shouldOpen()) {
            return;
        }
        try {
            this.database = new TimeSeriesDBSynchronousChannelDatabase(
                    TimeSeriesDBSynchronousChannel.class.getSimpleName());
        } catch (final Exception e) {
            throw new IOException("Unable to open the database: " + directory, e);
        }
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() == 1;
    }

    public synchronized ALiveSegmentedTimeSeriesDB<String, IndexedByteBuffer> getDatabase() {
        return database;
    }

    public String getHashKey() {
        return hashKey;
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    @Override
    public void close() throws IOException {
        if (!shouldClose()) {
            return;
        }
        if (database != null) {
            try {
                database.close();
                database = null;
            } catch (final Exception e) {
                throw new IOException("Unable to close the database: " + directory, e);
            }
        }
    }

    private final class TimeSeriesDBSynchronousChannelDatabase
            extends ALiveSegmentedTimeSeriesDB<String, IndexedByteBuffer> {
        private TimeSeriesDBSynchronousChannelDatabase(final String name) {
            super(name);
        }

        @Override
        protected String getElementsName() {
            return "channel values";
        }

        @Override
        public File getBaseDirectory() {
            return directory;
        }

        @Override
        public FDate extractStartTime(final IndexedByteBuffer value) {
            return value.getTime();
        }

        @Override
        protected ISerde<IndexedByteBuffer> newValueSerde() {
            return IndexedByteBufferSerde.GET;
        }

        @Override
        protected Integer newValueFixedLength() {
            return IndexedByteBufferSerde.getFixedLength(valueFixedLength);
        }

        @Override
        protected String innerHashKeyToString(final String key) {
            return key;
        }

        @Override
        public FDate extractEndTime(final IndexedByteBuffer value) {
            return value.getTime();
        }

        @Override
        protected ICloseableIterable<? extends IndexedByteBuffer> downloadSegmentElements(
                final SegmentedKey<String> segmentedKey) {
            return EmptyCloseableIterable.getInstance();
        }

        @Override
        public ISegmentFinder getSegmentFinder(final String key) {
            return DummySegmentFinder.INSTANCE;
        }

        @Override
        public FDate getFirstAvailableHistoricalSegmentFrom(final String key) {
            return PeriodicalSegmentFinder.BEFORE_DUMMY_RANGE.getFrom();
        }

        @Override
        public FDate getLastAvailableHistoricalSegmentTo(final String key, final FDate updateTo) {
            return PeriodicalSegmentFinder.BEFORE_DUMMY_RANGE.getTo();
        }

        @Override
        protected ICompressionFactory newCompressionFactory() {
            return TimeSeriesDBSynchronousChannel.this.newCompressionFactory();
        }
    }

}
