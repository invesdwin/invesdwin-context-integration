package de.invesdwin.context.integration.channel.sync.disni.passive;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.verbs.SVCPollCq;
import com.ibm.disni.verbs.SVCPostRecv;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class DisniPassiveSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private DisniPassiveSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer nioBuffer;
    private int bufferOffset = 0;
    private int messageTargetPosition = 0;
    private SVCPostRecv recvTask;
    private SVCPollCq poll;

    public DisniPassiveSynchronousReader(final DisniPassiveSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        nioBuffer = channel.getEndpoint().getRecvBuf();
        buffer = ByteBuffers.wrap(nioBuffer);

        poll = channel.getEndpoint().getPoll();
        recvTask = channel.getEndpoint().postRecv(channel.getEndpoint().getWrList_recv());
    }

    @Override
    public void close() {
        if (buffer != null) {
            recvTask.free();
            recvTask = null;

            buffer = null;
            nioBuffer = null;
            bufferOffset = 0;
            messageTargetPosition = 0;
        }

        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (buffer == null) {
            throw FastEOFException.getInstance("socket closed");
        }
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (messageTargetPosition == 0) {
            final int sizeTargetPosition = bufferOffset + DisniPassiveSynchronousChannel.MESSAGE_INDEX;
            //allow reading further than required to reduce the syscalls if possible
            if (!readFurther(sizeTargetPosition, buffer.remaining(nioBuffer.position()))) {
                return false;
            }
            final int size = buffer.getInt(bufferOffset + DisniPassiveSynchronousChannel.SIZE_INDEX);
            if (size <= 0) {
                close();
                throw FastEOFException.getInstance("non positive size");
            }
            this.messageTargetPosition = sizeTargetPosition + size;
            buffer.ensureCapacity(messageTargetPosition);
        }
        /*
         * only read as much further as required, so that we have a message where we can reset the position to 0 so the
         * expandable buffer does not grow endlessly due to fragmented messages piling up at the end each time.
         */
        return readFurther(messageTargetPosition, messageTargetPosition - nioBuffer.position());
    }

    private boolean readFurther(final int targetPosition, final int readLength) throws IOException {
        if (nioBuffer.position() < targetPosition) {
            if (poll.execute().getPolls() > 0) {
                //when there is no pending read, writes on the other side will never arrive
                recvTask.execute();
                //disni does not provide a way to give the received size, instead message are always received fully
                final int size = buffer.getInt(bufferOffset + DisniPassiveSynchronousChannel.SIZE_INDEX);
                if (size <= 0) {
                    close();
                    throw FastEOFException.getInstance("non positive size");
                }
                ByteBuffers.position(nioBuffer, bufferOffset + DisniPassiveSynchronousChannel.MESSAGE_INDEX + size);
            }
        }
        return nioBuffer.position() >= targetPosition;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = messageTargetPosition - bufferOffset - DisniPassiveSynchronousChannel.MESSAGE_INDEX;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + DisniPassiveSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + DisniPassiveSynchronousChannel.MESSAGE_INDEX, size);
        final int offset = DisniPassiveSynchronousChannel.MESSAGE_INDEX + size;
        if (nioBuffer.position() > (bufferOffset + offset)) {
            /*
             * can be a maximum of a few messages we read like this because of the size in hasNext, the next read in
             * hasNext will be done with position 0
             */
            bufferOffset += offset;
        } else {
            bufferOffset = 0;
            ByteBuffers.position(nioBuffer, 0);
        }
        messageTargetPosition = 0;
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
