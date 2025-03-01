package de.invesdwin.context.integration.channel.sync.pipe.service;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientChannel;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceListener;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.compression.CompressionSynchronousReader;
import de.invesdwin.context.integration.channel.sync.compression.CompressionSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.NativePipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.NativePipeSynchronousWriter;
import de.invesdwin.context.integration.compression.CompressionMode;
import de.invesdwin.context.integration.compression.lz4.LZ4Streams;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableSet;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class PipeStreamSynchronousEndpointService implements IStreamSynchronousEndpointService {

    private static final AtomicInteger UNIQUE_IDS = new AtomicInteger();

    //TODO: somehow enabled compression causes reader to throw EOFException after first read
    private static final CompressionMode DEFAULT_COMPRESSION_MODE = CompressionMode.NONE;
    private final int uniqueId;
    private final int serviceId;
    private final String topic;
    private final IFastIterableSet<IStreamSynchronousEndpointServiceListener> listeners = ILockCollectionFactory
            .getInstance(true)
            .newFastIterableIdentitySet();
    private final File file;
    private final ISynchronousWriter<IByteBufferProvider> writer;
    private Integer estimatedMaxMessageSize;
    private final CompressionMode compressionMode;

    public PipeStreamSynchronousEndpointService(final int serviceId, final String topic, final IProperties properties) {
        this.uniqueId = UNIQUE_IDS.incrementAndGet();
        this.serviceId = serviceId;
        this.topic = topic;
        this.file = newFile(topic);
        final Integer valueFixedLength = properties
                .getIntegerOptional(StreamSynchronousEndpointClientChannel.KEY_VALUE_FIXED_LENGTH, null);
        if (valueFixedLength != null) {
            this.estimatedMaxMessageSize = valueFixedLength;
        } else {
            this.estimatedMaxMessageSize = LZ4Streams.DEFAULT_BLOCK_SIZE_BYTES;
        }
        this.compressionMode = properties.getEnumOptional(CompressionMode.class,
                StreamSynchronousEndpointClientChannel.KEY_COMPRESSION_MODE, DEFAULT_COMPRESSION_MODE);
        this.writer = maybeCompress(newWriter(file));
    }

    public CompressionMode getCompressionMode() {
        return compressionMode;
    }

    public Integer getEstimatedMaxMessageSize() {
        return estimatedMaxMessageSize;
    }

    protected ISynchronousWriter<IByteBufferProvider> maybeCompress(
            final ISynchronousWriter<IByteBufferProvider> writer) {
        if (compressionMode == CompressionMode.NONE) {
            return writer;
        } else {
            return new CompressionSynchronousWriter(writer, compressionMode.newCompressionFactory());
        }
    }

    protected ISynchronousReader<IByteBufferProvider> maybeDecompress(
            final ISynchronousReader<IByteBufferProvider> reader) {
        if (compressionMode == CompressionMode.NONE) {
            return reader;
        } else {
            return new CompressionSynchronousReader(reader, compressionMode.newCompressionFactory());
        }
    }

    protected ISynchronousWriter<IByteBufferProvider> newWriter(final File file) {
        return new NativePipeSynchronousWriter(file, getEstimatedMaxMessageSize()) {
            @Override
            protected boolean newCloseMessageEnabled() {
                return false;
            }
        };
    }

    protected ISynchronousReader<IByteBufferProvider> newReader(final File file) {
        return new NativePipeSynchronousReader(file, getEstimatedMaxMessageSize()) {
            @Override
            protected boolean newCloseMessageEnabled() {
                return false;
            }
        };
    }

    @Override
    public void open() throws IOException {
        Files.forceMkdirParent(file);
        Files.touch(file);
        writer.open();
    }

    protected File newFile(final String topic) {
        return new File(newBaseFolder(), Files.normalizePath(uniqueId + "_" + topic));
    }

    protected File newBaseFolder() {
        return new File(ContextProperties.TEMP_DIRECTORY, PipeStreamSynchronousEndpointService.class.getSimpleName());
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
        final FDate fromTimestamp = parameters
                .getDateOptional(StreamSynchronousEndpointClientChannel.KEY_FROM_TIMESTAMP);
        if (fromTimestamp != null) {
            throw new IllegalArgumentException("fromTimestamp not supported by this storage");
        }
        if (listeners.add(listener)) {
            return maybeDecompress(newReader(file));
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
        Files.deleteQuietly(file);
        return true;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("uniqueId", uniqueId)
                .add("serviceId", serviceId)
                .add("topic", topic)
                .toString();
    }

}
