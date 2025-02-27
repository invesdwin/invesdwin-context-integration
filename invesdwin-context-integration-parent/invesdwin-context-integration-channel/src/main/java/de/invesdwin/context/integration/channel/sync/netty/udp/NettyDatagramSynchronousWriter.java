package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeChannelPromise;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;

@NotThreadSafe
public class NettyDatagramSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private NettyDatagramSynchronousChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private DatagramPacket datagramPacket;
    private Runnable writer;
    private ChannelFuture future;

    public NettyDatagramSynchronousWriter(final NettyDatagramSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open(bootstrap -> {
            bootstrap.handler(new ChannelInboundHandlerAdapter());
        }, null);
        final boolean safeWriter = isSafeWriter(channel);
        if (safeWriter) {
            writer = () -> {
                //TODO: implement a writer that can push messages without waiting on the future (just allocate more buffers)
                future = channel.getDatagramChannel().writeAndFlush(datagramPacket);
            };
        } else {
            channel.getDatagramChannel().deregister();
            channel.closeBootstrapAsync();
            FakeEventLoop.INSTANCE.register(channel.getDatagramChannel());
            writer = () -> {
                channel.getDatagramChannel().unsafe().write(datagramPacket, FakeChannelPromise.INSTANCE);
                channel.getDatagramChannel().unsafe().flush();
            };
        }
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(channel.getSocketSize());
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettyDatagramSynchronousChannel.MESSAGE_INDEX);
        if (!channel.isServer()) {
            //client immediately knows where to send requests to
            this.datagramPacket = new DatagramPacket(buf, channel.getSocketAddress());
        }
    }

    protected boolean isSafeWriter(final NettyDatagramSynchronousChannel channel) {
        //        final DatagramChannel datagramChannel = channel.getDatagramChannel();
        //        return datagramChannel instanceof io.netty.channel.socket.oio.OioDatagramChannel
        //                || datagramChannel instanceof NioDatagramChannel || datagramChannel instanceof IOUringDatagramChannel
        //                || channel.isKeepBootstrapRunningAfterOpen();
        //unsafe interface will be removed in netty5, also unsafe makes tests flaky
        return true;
    }

    @Override
    public void close() {
        final NettyDatagramSynchronousChannel channelCopy = channel;
        if (buffer != null) {
            if (channelCopy != null) {
                if (!channelCopy.isServer() || !channelCopy.isMultipleClientsAllowed() && datagramPacket != null) {
                    try {
                        writeFuture(ClosedByteBuffer.INSTANCE);
                    } catch (final Throwable t) {
                        //ignore
                    }
                }
            }
            buf.release();
            buf = null;
            buffer = null;
            messageBuffer = null;
            datagramPacket = null;
            writer = null;
        }
        if (channelCopy != null) {
            channelCopy.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        if (future == null) {
            return true;
        }
        if (future.isDone()) {
            future = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        writeFuture(message);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

    private void writeFuture(final IByteBufferProvider message) throws IOException {
        if (datagramPacket == null) {
            //server needs to know where to respond to
            this.datagramPacket = new DatagramPacket(buf, channel.getOtherSocketAddress());
        } else if (channel.isMultipleClientsAllowed()
                && !datagramPacket.recipient().equals(channel.getOtherSocketAddress())) {
            this.datagramPacket.release();
            this.datagramPacket = new DatagramPacket(buf, channel.getOtherSocketAddress());
        }

        buf.setIndex(0, 0); //reset indexes
        final int size = message.getBuffer(messageBuffer);
        buffer.putInt(NettyDatagramSynchronousChannel.SIZE_INDEX, size);
        buf.setIndex(0, NettyDatagramSynchronousChannel.MESSAGE_INDEX + size);
        datagramPacket.retain(); //keep retain count up
        writer.run();
    }

}
