package de.invesdwin.context.integration.channel.sync.disni;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.verbs.IbvCQ;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.IbvSge;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.SVCPollCq;
import com.ibm.disni.verbs.SVCPostRecv;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class DisniSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private DisniSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer nioBuffer;
    private int bufferOffset = 0;
    private int messageTargetPosition = 0;
    private SVCPostRecv recvTask;
    private IbvWC[] wcList;
    private SVCPollCq poll;

    public DisniSynchronousReader(final DisniSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirect(socketSize);
        nioBuffer = buffer.nioByteBuffer();

        final IbvCQ cq = channel.getEndpoint().getCqProvider().getCQ();
        //        Assertions.checkEquals(1, channel.getEndpoint().getCqProvider().getCqSize());
        this.wcList = new IbvWC[channel.getEndpoint().getCqProvider().getCqSize()];
        for (int i = 0; i < wcList.length; i++) {
            wcList[i] = new IbvWC();
        }
        this.poll = cq.poll(wcList, wcList.length);

        recvTask = setupRecvTask(nioBuffer, 0);
        //when there is no pending read, writes on the other side will never arrive
        recvTask.execute();
    }

    private SVCPostRecv setupRecvTask(final java.nio.ByteBuffer recvBuf, final int wrid) throws IOException {
        final List<IbvRecvWR> recvWRs = new ArrayList<IbvRecvWR>(1);
        final LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

        final IbvMr mr = channel.getEndpoint().registerMemory(recvBuf).execute().free().getMr();
        channel.getMemoryRegions().push(mr);
        final IbvSge sge = new IbvSge();
        sge.setAddr(mr.getAddr());
        sge.setLength(mr.getLength());
        final int lkey = mr.getLkey();
        sge.setLkey(lkey);
        sgeList.add(sge);

        final IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setWr_id(wrid);
        recvWR.setSg_list(sgeList);
        recvWRs.add(recvWR);

        return channel.getEndpoint().postRecv(recvWRs);
    }

    @Override
    public void close() {
        if (buffer != null) {
            recvTask.free();
            recvTask = null;
            wcList = null;
            poll.free();
            poll = null;

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
            final int sizeTargetPosition = bufferOffset + DisniSynchronousChannel.MESSAGE_INDEX;
            //allow reading further than required to reduce the syscalls if possible
            if (!readFurther(sizeTargetPosition, buffer.remaining(nioBuffer.position()))) {
                return false;
            }
            final int size = buffer.getInt(bufferOffset + DisniSynchronousChannel.SIZE_INDEX);
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
                final int size = buffer.getInt(bufferOffset + DisniSynchronousChannel.SIZE_INDEX);
                if (size <= 0) {
                    close();
                    throw FastEOFException.getInstance("non positive size");
                }
                ByteBuffers.position(nioBuffer, bufferOffset + DisniSynchronousChannel.MESSAGE_INDEX + size);
            }
        }
        return nioBuffer.position() >= targetPosition;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = messageTargetPosition - bufferOffset - DisniSynchronousChannel.MESSAGE_INDEX;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + DisniSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + DisniSynchronousChannel.MESSAGE_INDEX, size);
        final int offset = DisniSynchronousChannel.MESSAGE_INDEX + size;
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
