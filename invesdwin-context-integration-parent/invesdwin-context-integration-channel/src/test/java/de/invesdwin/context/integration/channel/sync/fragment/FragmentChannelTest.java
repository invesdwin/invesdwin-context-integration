package de.invesdwin.context.integration.channel.sync.fragment;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class FragmentChannelTest extends ALatencyChannelTest {

    @Test
    public void testFragmentPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.BLOCKING_MAPPED;
        final File requestFile = newFile("testFragmentPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testFragmentPerformance_response.pipe", tmpfs, pipes);
        runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        return new FragmentSynchronousReader(super.newReader(file, pipes));
    }

    @Override
    protected ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        return new FragmentSynchronousWriter(super.newWriter(file, pipes), getMaxMessageSize());
    }

    @Override
    protected int getMaxMessageSize() {
        return FragmentSynchronousWriter.PAYLOAD_INDEX + 1;
    }

}
