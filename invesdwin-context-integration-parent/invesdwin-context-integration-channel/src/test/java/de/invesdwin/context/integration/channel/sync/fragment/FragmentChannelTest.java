package de.invesdwin.context.integration.channel.sync.fragment;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class FragmentChannelTest extends AChannelTest {

    @Test
    public void testFragmentPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.BLOCKING_MAPPED;
        final File requestFile = newFile("testFragmentPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testFragmentPerformance_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        return new FragmentSynchronousReader(super.newReader(file, pipes));
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        return new FragmentSynchronousWriter(super.newWriter(file, pipes), getMaxMessageSize());
    }

    @Override
    public int getMaxMessageSize() {
        return FragmentSynchronousWriter.PAYLOAD_INDEX + 1;
    }

}
