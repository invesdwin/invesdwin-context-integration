package de.invesdwin.context.integration.channel.sync.timeseriesdb.service;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceListener;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.timeseriesdb.TimeSeriesDBSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.timeseriesdb.TimeSeriesDBSynchronousChannelIndexMode;
import de.invesdwin.context.integration.channel.sync.timeseriesdb.TimeSeriesDBSynchronousReader;
import de.invesdwin.context.integration.channel.sync.timeseriesdb.TimeSeriesDBSynchronousWriter;
import de.invesdwin.context.integration.compression.CompressionMode;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableSet;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class TimeSeriesDBStreamSynchronousEndpointService implements IStreamSynchronousEndpointService {

    public static final String KEY_COMPRESSION_MODE = "COMPRESSION_MODE";
    public static final String KEY_VALUE_FIXED_LENGTH = "VALUE_FIXED_LENGTH";
    public static final String KEY_FROM_TIMESTAMP = "FROM_TIMESTAMP";

    private final int serviceId;
    private final String topic;
    private final IFastIterableSet<IStreamSynchronousEndpointServiceListener> listeners = ILockCollectionFactory
            .getInstance(true)
            .newFastIterableIdentitySet();
    private final TimeSeriesDBSynchronousChannel channel;
    private final TimeSeriesDBSynchronousWriter writer;

    public TimeSeriesDBStreamSynchronousEndpointService(final int serviceId, final String topic,
            final IProperties properties) {
        this.serviceId = serviceId;
        this.topic = topic;
        final Integer valueFixedLength = properties.getIntegerOptional(KEY_VALUE_FIXED_LENGTH, null);
        final CompressionMode compressionMode = properties.getEnumOptional(CompressionMode.class, KEY_COMPRESSION_MODE,
                CompressionMode.DEFAULT);
        this.channel = new TimeSeriesDBSynchronousChannel(newFolder(), valueFixedLength) {
            @Override
            protected boolean newCloseMessageEnabled() {
                return false;
            }

            @Override
            protected TimeSeriesDBSynchronousChannelIndexMode newIndexMode() {
                return TimeSeriesDBSynchronousChannelIndexMode.TIME;
            }

            @Override
            public ICompressionFactory newCompressionFactory() {
                return compressionMode.newCompressionFactory();
            }
        };
        this.writer = new TimeSeriesDBSynchronousWriter(channel);
    }

    @Override
    public void open() throws IOException {
        channel.open();
        writer.open();
    }

    private File newFolder() {
        return new File(ContextProperties.getCacheDirectory(),
                TimeSeriesDBStreamSynchronousEndpointService.class.getSimpleName());
    }

    @Override
    public int getServiceId() {
        return serviceId;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public boolean put(final IByteBufferProvider message) throws Exception {
        synchronized (writer) {
            Assertions.checkTrue(writer.writeReady());
            Assertions.checkTrue(writer.writeAndFlushIfPossible(message));
        }
        final IStreamSynchronousEndpointServiceListener[] listenersArray = listeners
                .asArray(IStreamSynchronousEndpointServiceListener.EMPTY_ARRAY);
        for (int i = 0; i < listenersArray.length; i++) {
            final IStreamSynchronousEndpointServiceListener listener = listenersArray[i];
            listener.onPut();
        }
        return true;
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> subscribe(final IStreamSynchronousEndpointServiceListener listener,
            final IProperties parameters) {
        final FDate fromTimestamp = parameters.getDateOptional(KEY_FROM_TIMESTAMP);
        if (listeners.add(listener)) {
            return new TimeSeriesDBSynchronousReader(channel) {
                @Override
                protected long newIgnoreValuesBeforeIndex() {
                    if (fromTimestamp != null) {
                        return fromTimestamp.millisValue();
                    } else {
                        return super.newIgnoreValuesBeforeIndex();
                    }
                }
            };
        } else {
            return null;
        }
    }

    @Override
    public boolean unsubscribe(final IStreamSynchronousEndpointServiceListener listener, final IProperties parameters) {
        return listeners.remove(listener);
    }

    @Override
    public boolean delete(final IProperties parameters) throws Exception {
        final IStreamSynchronousEndpointServiceListener[] listenersArray = listeners
                .asArray(IStreamSynchronousEndpointServiceListener.EMPTY_ARRAY);
        for (int i = 0; i < listenersArray.length; i++) {
            final IStreamSynchronousEndpointServiceListener listener = listenersArray[i];
            listener.onDelete(parameters);
        }
        channel.getDatabase().deleteRange(channel.getHashKey());
        return true;
    }

    @Override
    public void close() throws IOException {
        writer.close();
        channel.close();
    }

}
