package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import java.io.IOException;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.timed.IndexedByteBuffer;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@NotThreadSafe
public class TimeSeriesDBSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final TimeSeriesDBSynchronousChannel channel;
    private ALiveSegmentedTimeSeriesDB<String, IndexedByteBuffer> database;
    private long lastIndex = FDates.MIN_DATE.millisValue();
    private ICloseableIterator<IndexedByteBuffer> readRange;
    private FDate readRangeToIndex;
    private IndexedByteBuffer message;

    /**
     * TODO: we should let readers decide from what message index to start from; it might also make sense to offer a
     * reader/writer option that use actual timestamps instead of slightly faster indexes so that the decision from
     * which message to start from can be made based on time, not just on index.
     */
    public TimeSeriesDBSynchronousReader(final TimeSeriesDBSynchronousChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        this.database = channel.getDatabase();
    }

    @Override
    public void close() throws IOException {
        if (readRange != null) {
            readRange.close();
            readRange = null;
        }
        if (database != null) {
            database.close();
            database = null;
            channel.close();
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (message != null) {
            return true;
        }
        if (readRange == null) {
            readFirst();
        } else {
            try {
                message = readRange.next();
            } catch (final NoSuchElementException e) {
                readRange.close();
                readRange = null;
                readFirst();
            }
        }
        return message != null;
    }

    private void readFirst() {
        readRangeToIndex = database.getLatestValueKey(channel.getKey(), FDates.MAX_DATE);
        if (readRangeToIndex == null || readRangeToIndex.millisValue() <= lastIndex) {
            //no additional data available yet
            return;
        }
        readRange = database.rangeValues(channel.getKey(), new FDate(lastIndex + 1), readRangeToIndex).iterator();
        try {
            message = readRange.next();
        } catch (final NoSuchElementException e1) {
            readRange = null;
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        lastIndex = message.getIndex();
        return message.getByteBuffer();
    }

    @Override
    public void readFinished() {
        message = null;
    }

}
