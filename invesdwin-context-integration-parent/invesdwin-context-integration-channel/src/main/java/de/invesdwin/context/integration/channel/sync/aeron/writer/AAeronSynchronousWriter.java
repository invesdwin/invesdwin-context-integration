package de.invesdwin.context.integration.channel.sync.aeron.writer;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.aeron.AAeronSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.aeron.AeronInstance;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import io.aeron.Publication;

@NotThreadSafe
public abstract class AAeronSynchronousWriter extends AAeronSynchronousChannel {

    protected boolean connected = false;
    private final MutableReservedValueSupplier reservedValueSupplier = new MutableReservedValueSupplier();

    public AAeronSynchronousWriter(final AeronInstance instance, final String channel, final int streamId) {
        super(instance, channel, streamId);
    }

    protected boolean tryOffer(final Publication publication, final IByteBuffer buffer, final int size)
            throws IOException {
        reservedValueSupplier.setReservedValue(size);
        final long result = publication.offer(buffer.directBuffer(), 0, size, reservedValueSupplier);
        return handleResult(result);
    }

    protected boolean handleResult(final long result) throws IOException {
        if (result <= 0) {
            if (result == Publication.NOT_CONNECTED) {
                if (connected) {
                    connected = false;
                    close();
                    throw FastEOFException.getInstance("closed by other side: NOT_CONNECTED=%s", result);
                } else {
                    return false;
                }
            } else if (result == Publication.CLOSED) {
                close();
                throw FastEOFException.getInstance("closed by other side: CLOSED=%s", result);
            } else if (result == Publication.MAX_POSITION_EXCEEDED) {
                close();
                throw FastEOFException.getInstance("closed by other side: MAX_POSITION_EXCEEDED=%s", result);
            } else if (result == 0 || result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
                return false;
            } else {
                throw UnknownArgumentException.newInstance(Long.class, result);
            }
        } else {
            connected = true;
            return true;
        }
    }

}
