// CHECKSTYLE:OFF

package de.invesdwin.context.integration.channel.sync.neutrino.examples;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.AccessFlag;
import de.hhu.bsinfo.neutrino.verbs.CompletionChannel;
import de.hhu.bsinfo.neutrino.verbs.CompletionQueue;
import de.hhu.bsinfo.neutrino.verbs.PortAttributes;
import de.hhu.bsinfo.neutrino.verbs.QueuePair;

@NotThreadSafe
public class ConnectionContext extends BaseContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionContext.class);

    private final RegisteredBuffer localBuffer;
    private final CompletionQueue completionQueue;
    private final CompletionChannel completionChannel;

    private final QueuePair queuePair;

    private final PortAttributes port;

    public ConnectionContext(final int deviceNumber, final int queueSize, final long messageSize) throws IOException {
        super(deviceNumber);

        port = getContext().queryPort(1);
        if (port == null) {
            throw new IOException("Unable to query port");
        }

        localBuffer = getProtectionDomain().allocateMemory(messageSize, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ,
                AccessFlag.REMOTE_WRITE, AccessFlag.MW_BIND);
        if (localBuffer == null) {
            throw new IOException("Unable to allocate message buffer");
        }

        LOGGER.info("Allocated registered memory buffer");

        completionChannel = getContext().createCompletionChannel();
        if (completionChannel == null) {
            throw new IOException("Unable to create completion channel");
        }

        LOGGER.info("Created completion channel");

        completionQueue = getContext().createCompletionQueue(queueSize, completionChannel);
        if (completionQueue == null) {
            throw new IOException("Unable to create completion queue");
        }

        LOGGER.info("Created completion queue");

        queuePair = getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(QueuePair.Type.RC,
                completionQueue, completionQueue, queueSize, queueSize, 1, 1).build());
        if (queuePair == null) {
            throw new IOException("Unable to create queue pair");
        }

        LOGGER.info("Created queue pair");

        queuePair.modify(QueuePair.Attributes.Builder.buildInitAttributesRC((short) 0, (byte) 1, AccessFlag.LOCAL_WRITE,
                AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE));

        LOGGER.info("Moved queue pair into INIT state");
    }

    public void connect(final Socket socket) throws IOException {
        final ConnectionInformation localInfo = new ConnectionInformation((byte) 1, port.getLocalId(),
                queuePair.getQueuePairNumber());

        LOGGER.info("Local connection information: {}", localInfo);

        socket.getOutputStream()
                .write(ByteBuffer.allocate(Byte.BYTES + Short.BYTES + Integer.BYTES)
                        .put(localInfo.getPortNumber())
                        .putShort(localInfo.getLocalId())
                        .putInt(localInfo.getQueuePairNumber())
                        .array());

        LOGGER.info("Waiting for remote connection information");

        final ByteBuffer byteBuffer = ByteBuffer
                .wrap(socket.getInputStream().readNBytes(Byte.BYTES + Short.BYTES + Integer.BYTES));
        final ConnectionInformation remoteInfo = new ConnectionInformation(byteBuffer);

        LOGGER.info("Received connection information: {}", remoteInfo);

        queuePair.modify(QueuePair.Attributes.Builder.buildReadyToReceiveAttributesRC(remoteInfo.getQueuePairNumber(),
                remoteInfo.getLocalId(), remoteInfo.getPortNumber()));

        LOGGER.info("Moved queue pair into RTR state");

        queuePair.modify(QueuePair.Attributes.Builder.buildReadyToSendAttributesRC());

        LOGGER.info("Moved queue pair into RTS state");
    }

    public RegisteredBuffer getLocalBuffer() {
        return localBuffer;
    }

    public CompletionQueue getCompletionQueue() {
        return completionQueue;
    }

    public CompletionChannel getCompletionChannel() {
        return completionChannel;
    }

    public QueuePair getQueuePair() {
        return queuePair;
    }

    @Override
    public void close() throws IOException {
        queuePair.close();
        completionQueue.close();
        completionChannel.close();
        localBuffer.close();
        super.close();

        LOGGER.info("Closed context");
    }
}
