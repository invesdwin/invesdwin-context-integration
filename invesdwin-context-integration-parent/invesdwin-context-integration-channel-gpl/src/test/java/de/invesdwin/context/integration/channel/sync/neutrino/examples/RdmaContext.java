// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.sync.neutrino.examples;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.StringJoiner;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class RdmaContext extends ConnectionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(RdmaContext.class);

    private BufferInformation remoteBufferInfo;

    public RdmaContext(final int deviceNumber, final int queueSize, final long bufferSize) throws IOException {
        super(deviceNumber, queueSize, bufferSize);
    }

    @Override
    public void connect(final Socket socket) throws IOException {
        super.connect(socket);

        final BufferInformation localBufferInfo = new BufferInformation(getLocalBuffer().getHandle(),
                getLocalBuffer().getRemoteKey());

        LOGGER.info("Local buffer information: {}", localBufferInfo);

        socket.getOutputStream()
                .write(ByteBuffer.allocate(Long.BYTES + Integer.BYTES)
                        .putLong(localBufferInfo.getAddress())
                        .putInt(localBufferInfo.getRemoteKey())
                        .array());

        LOGGER.info("Waiting for remote buffer information");

        final ByteBuffer byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(Long.BYTES + Integer.BYTES));
        remoteBufferInfo = new BufferInformation(byteBuffer);

        LOGGER.info("Received buffer information: {}", remoteBufferInfo);
    }

    public BufferInformation getRemoteBufferInfo() {
        return remoteBufferInfo;
    }

    public static final class BufferInformation {

        private final long address;
        private final int remoteKey;

        BufferInformation(final long address, final int remoteKey) {
            this.address = address;
            this.remoteKey = remoteKey;
        }

        BufferInformation(final ByteBuffer buffer) {
            address = buffer.getLong();
            remoteKey = buffer.getInt();
        }

        public long getAddress() {
            return address;
        }

        public int getRemoteKey() {
            return remoteKey;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", BufferInformation.class.getSimpleName() + "[", "]").add("address=" + address)
                    .add("remoteKey=" + remoteKey)
                    .toString();
        }
    }
}
