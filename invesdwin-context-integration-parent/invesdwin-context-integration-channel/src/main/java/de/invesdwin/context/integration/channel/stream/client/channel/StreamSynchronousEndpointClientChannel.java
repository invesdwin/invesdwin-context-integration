package de.invesdwin.context.integration.channel.stream.client.channel;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.client.IStreamSynchronousEndpointClient;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.compression.CompressionMode;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class StreamSynchronousEndpointClientChannel implements ISynchronousChannel {

    public static final String KEY_COMPRESSION_MODE = "COMPRESSION_MODE";
    public static final String KEY_VALUE_FIXED_LENGTH = "VALUE_FIXED_LENGTH";
    public static final String KEY_FROM_TIMESTAMP = "FROM_TIMESTAMP";

    private final IStreamSynchronousEndpointClient client;
    private final Integer valueFixedLength;
    private final CompressionMode compressionMode;
    private final boolean closeMessageEnabled;
    private final String topic;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private final int serviceId;

    public StreamSynchronousEndpointClientChannel(final IStreamSynchronousEndpointClient client, final String topic,
            final Integer valueFixedLength) {
        this.client = client;
        this.valueFixedLength = valueFixedLength;
        this.compressionMode = newCompressionMode();
        this.closeMessageEnabled = newCloseMessageEnabled();
        this.topic = topic;
        this.serviceId = client.newServiceId(topic);
    }

    public IStreamSynchronousEndpointClient getClient() {
        return client;
    }

    public Integer getValueFixedLength() {
        return valueFixedLength;
    }

    public String getTopic() {
        return topic;
    }

    public int getServiceId() {
        return serviceId;
    }

    protected CompressionMode newCompressionMode() {
        return CompressionMode.DEFAULT;
    }

    public CompressionMode getCompressionMode() {
        return compressionMode;
    }

    protected boolean newCloseMessageEnabled() {
        return true;
    }

    public boolean isCloseMessageEnabled() {
        return closeMessageEnabled;
    }

    @Override
    public synchronized void open() throws IOException {
        if (!shouldOpen()) {
            return;
        }
        client.open();
        onOpen();
    }

    protected void onOpen() {
        client.create(serviceId, newCreateTopicUri());
    }

    public String newCreateTopicUri() {
        final StringBuilder uri = new StringBuilder(topic);
        uri.append("?");
        uri.append(KEY_COMPRESSION_MODE);
        uri.append("=");
        uri.append(getCompressionMode());
        if (getValueFixedLength() != null) {
            uri.append("&");
            uri.append(KEY_VALUE_FIXED_LENGTH);
            uri.append("=");
            uri.append(getValueFixedLength());
        }
        return uri.toString();
    }

    public String newSubscribeTopicUri(final FDate fromTimestamp) {
        if (fromTimestamp == null) {
            return topic;
        } else {
            return topic + "?" + KEY_FROM_TIMESTAMP + "=" + fromTimestamp;
        }
    }

    public String newUnsubscribeTopicUri() {
        return topic;
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() == 1;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!shouldClose()) {
            return;
        }
        /*
         * TODO: maybe delete topic here (based on configuration), should also offer a function in the client and server
         * to list all topics (checking both inactive ones on disk and server state in memory) to clean up everything if
         * desired
         */
        client.close();
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

}
