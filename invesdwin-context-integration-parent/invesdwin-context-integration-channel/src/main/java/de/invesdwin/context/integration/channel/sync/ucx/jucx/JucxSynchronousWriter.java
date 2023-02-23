package de.invesdwin.context.integration.channel.sync.ucx.jucx;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.openucx.jucx.ucp.UcpMemory;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class JucxSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private JucxSynchronousChannel channel;
    private UcpMemory memory;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private long messageToWrite;
    private int position;
    private int remaining;

    public JucxSynchronousWriter(final JucxSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        memory = channel.getUcpContext().memoryMap(channel.getUcpMemMapParams());
        buffer = new UnsafeByteBuffer(memory.getAddress(), channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, SocketSynchronousChannel.MESSAGE_INDEX);
    }

    @Override
    public void close() {
        if (buffer != null) {
            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            memory.close();
            buffer = null;
            messageBuffer = null;
            messageToWrite = 0;
            position = 0;
            remaining = 0;
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
            buffer.putInt(SocketSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.addressOffset();
            position = 0;
            remaining = SocketSynchronousChannel.MESSAGE_INDEX + size;
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
            position = 0;
            remaining = 0;
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        //        final long tag = TagUtil.setMessageType(remoteTag, TagUtil.MessageType.DEFAULT);
        //        final boolean completed = channel.getUcpEndpoint()
        //                .receiveTaggedMessage(buffer.addressOffset() + position, remaining, tag, TagUtil.TAG_MASK_FULL, true,
        //                        false);
        //        final int count = write0(fd, messageToWrite, position, remaining);
        //        remaining -= count;
        //        position += count;
        return remaining > 0;
    }

}
