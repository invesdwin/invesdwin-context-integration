package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.timed.IndexedByteBuffer;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;

@NotThreadSafe
public class TimeSeriesDBSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final TimeSeriesDBSynchronousChannel channel;
    private final TimeSeriesDBSynchronousChannelIndexMode mode;
    private long lastIndex = Long.MIN_VALUE;
    private ALiveSegmentedTimeSeriesDB<String, IndexedByteBuffer> database;

    public TimeSeriesDBSynchronousWriter(final TimeSeriesDBSynchronousChannel channel) {
        this.channel = channel;
        this.mode = channel.getIndexMode();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        database = channel.getDatabase();
        final FDate lastTime = database.getLatestValueKey(channel.getHashKey(), FDates.MAX_DATE);
        if (lastTime == null) {
            lastIndex = mode.initialIndex();
        } else {
            lastIndex = lastTime.millisValue();
        }
    }

    @Override
    public void close() throws IOException {
        if (database != null) {
            lastIndex = Long.MIN_VALUE;
            database = null;
            channel.close();
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        lastIndex = mode.nextIndex(lastIndex);
        final IByteBuffer messageCopy = ByteBuffers.wrap(message.asBuffer().asByteArrayCopy());
        database.putNextLiveValue(channel.getHashKey(), new IndexedByteBuffer(lastIndex, messageCopy));
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
