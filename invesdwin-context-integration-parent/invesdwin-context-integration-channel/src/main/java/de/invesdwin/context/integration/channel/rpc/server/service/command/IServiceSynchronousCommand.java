package de.invesdwin.context.integration.channel.rpc.server.service.command;

import java.io.Closeable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.StringUtf8Serde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

public interface IServiceSynchronousCommand<M> extends Closeable {

    /**
     * "HEARTBEAT" in numbers is "834578347"
     */
    int HEARTBEAT_SERVICE_ID = -834578347;

    /**
     * "ERROR" in numbers is "35505"
     */
    int ERROR_METHOD_ID = -35505;

    /**
     * "RETRERROR" in numbers is "537535505"
     */
    int RETRY_ERROR_METHOD_ID = -537535505;

    ISerde<String> ERROR_RESPONSE_SERDE = StringUtf8Serde.GET;
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ISerde<Object> ERROR_RESPONSE_SERDE_OBJ = (ISerde) ERROR_RESPONSE_SERDE;

    int getService();

    int getMethod();

    int getSequence();

    M getMessage();

    default int messageToBuffer(final ISerde<M> messageSerde, final IByteBuffer buffer) {
        return messageSerde.toBuffer(buffer, getMessage());
    }

}
