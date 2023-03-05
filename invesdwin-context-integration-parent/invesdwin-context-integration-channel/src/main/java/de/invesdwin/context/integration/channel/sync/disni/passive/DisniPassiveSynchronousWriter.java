package de.invesdwin.context.integration.channel.sync.disni.passive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.verbs.IbvCQ;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.SVCPollCq;
import com.ibm.disni.verbs.SVCPostSend;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class DisniPassiveSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private DisniPassiveSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer nioBuffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private long messageToWrite;
    private SVCPostSend sendTask;
    private boolean request;
    private IbvWC[] wcList;
    private SVCPollCq poll;

    public DisniPassiveSynchronousWriter(final DisniPassiveSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, DisniPassiveSynchronousChannel.MESSAGE_INDEX);
        nioBuffer = buffer.nioByteBuffer();

        sendTask = setupSendTask(nioBuffer, 0);

        final IbvCQ cq = channel.getEndpoint().getCqProvider().getCQ();
        //        Assertions.checkEquals(1, channel.getEndpoint().getCqProvider().getCqSize());
        this.wcList = new IbvWC[channel.getEndpoint().getCqProvider().getCqSize()];
        for (int i = 0; i < wcList.length; i++) {
            wcList[i] = new IbvWC();
        }
        this.poll = cq.poll(wcList, wcList.length);
    }

    private SVCPostSend setupSendTask(final java.nio.ByteBuffer sendBuf, final int wrid) throws IOException {
        final ArrayList<IbvSendWR> sendWRs = new ArrayList<IbvSendWR>(1);
        final LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

        final IbvMr mr = channel.getEndpoint().registerMemory(sendBuf).execute().free().getMr();
        channel.getMemoryRegions().push(mr);
        final IbvSge sge = new IbvSge();
        sge.setAddr(mr.getAddr());
        sge.setLength(mr.getLength());
        final int lkey = mr.getLkey();
        sge.setLkey(lkey);
        sgeList.add(sge);

        final IbvSendWR sendWR = new IbvSendWR();
        sendWR.setSg_list(sgeList);
        sendWR.setWr_id(wrid);
        sendWRs.add(sendWR);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());

        return channel.getEndpoint().postSend(sendWRs);
    }

    @Override
    public void close() {
        if (buffer != null) {
            sendTask.free();
            sendTask = null;
            request = false;
            wcList = null;
            poll.free();
            poll = null;

            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            nioBuffer = null;
            messageBuffer = null;
            messageToWrite = 0;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(DisniPassiveSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.addressOffset();
            ByteBuffers.position(nioBuffer, 0);
            ByteBuffers.limit(nioBuffer, DisniPassiveSynchronousChannel.MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (messageToWrite == 0) {
            return true;
        } else if (!writeFurther()) {
            messageToWrite = 0;
            nioBuffer.clear();
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        if (!request) {
            sendTask.execute();
            request = true;
        }
        if (poll.execute().getPolls() == 0) {
            return true;
        }
        request = false;
        return false;
    }

}
