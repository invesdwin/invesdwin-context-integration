package de.invesdwin.context.integration.channel;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.async.serde.SerdeAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.report.DisabledLatencyReportFactory;
import de.invesdwin.context.integration.channel.report.ILatencyReportFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.mapped.MappedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.mapped.MappedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mapped.blocking.BlockingMappedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.mapped.blocking.BlockingMappedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.PipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.PipeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.streaming.StreamingPipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.streaming.StreamingPipeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.NativePipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.NativePipeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.serde.SerdeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.serde.SerdeSynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.add.AddUndefinedBytesDelegateSerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class AChannelTest extends ATest {

    //one warmup/connect message
    public static final int WARMUP_MESSAGE_COUNT = 1;
    public static final boolean DEBUG = false;
    public static final int SIMULATED_ADDITONAL_MESSAGE_SIZE = 0;
    public static final int MIN_MESSAGE_SIZE = FDateSerde.FIXED_LENGTH;
    public static final int MAX_MESSAGE_SIZE = MIN_MESSAGE_SIZE + SIMULATED_ADDITONAL_MESSAGE_SIZE;
    public static final int MESSAGE_COUNT = DEBUG ? 10 : 1000000;
    public static final Duration MAX_WAIT_DURATION = new Duration(10, DEBUG ? FTimeUnit.DAYS : FTimeUnit.SECONDS);
    public static final ILatencyReportFactory LATENCY_REPORT_FACTORY = DisabledLatencyReportFactory.INSTANCE;

    public enum FileChannelType {
        PIPE_STREAMING,
        PIPE_NATIVE,
        PIPE,
        MAPPED,
        BLOCKING_MAPPED,
        UNIX_SOCKET;
    }

    public static String findLocalNetworkAddress() {
        for (final InetAddress localAddress : NetworkUtil.getLocalAddresses()) {
            final String localNetworkIp = localAddress.getHostAddress();
            if (localNetworkIp.startsWith("192.")) {
                return localNetworkIp;
            }
        }
        throw new IllegalStateException("no suitable network interface found");
    }

    protected File newFile(final String name, final boolean tmpfs, final FileChannelType type) {
        final File baseFolder = getBaseFolder(tmpfs);
        final File file = new File(baseFolder, name);
        Files.deleteQuietly(file);
        Assertions.checkFalse(file.exists(), "%s", file);
        if (type == FileChannelType.UNIX_SOCKET) {
            return file;
        } else if (type == FileChannelType.PIPE || type == FileChannelType.PIPE_STREAMING
                || type == FileChannelType.PIPE_NATIVE) {
            if (SynchronousChannels.isNamedPipeSupported() && !OperatingSystem.isWindows()) {
                Assertions.checkTrue(SynchronousChannels.createNamedPipe(file));
            } else {
                Files.touchQuietly(file);
            }
        } else if (type == FileChannelType.MAPPED || type == FileChannelType.BLOCKING_MAPPED) {
            Files.touchQuietly(file);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, type);
        }
        Assertions.checkTrue(file.exists());
        return file;
    }

    protected File newFolder(final String name, final boolean tmpfs) {
        final File baseFolder = getBaseFolder(tmpfs);
        final File folder = new File(baseFolder, name);
        try {
            Files.forceMkdir(folder);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return folder;
    }

    protected File getBaseFolder(final boolean tmpfs) {
        if (tmpfs) {
            return SynchronousChannels.getTmpfsFolderOrFallback();
        } else {
            return ContextProperties.TEMP_DIRECTORY;
        }
    }

    protected <T> ISynchronousReader<T> maybeSynchronize(final ISynchronousReader<T> reader, final Object synchronize) {
        if (synchronize != null) {
            return SynchronousChannels.synchronize(reader, synchronize);
        } else {
            return reader;
        }
    }

    protected <T> ISynchronousWriter<T> maybeSynchronize(final ISynchronousWriter<T> writer, final Object synchronize) {
        if (synchronize != null) {
            return SynchronousChannels.synchronize(writer, synchronize);
        } else {
            return writer;
        }
    }

    protected ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE_NATIVE) {
            return new NativePipeSynchronousReader(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.PIPE_STREAMING) {
            return new StreamingPipeSynchronousReader(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousReader(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousReader(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.BLOCKING_MAPPED) {
            return new BlockingMappedSynchronousReader(file, getMaxMessageSize());
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    protected int getMaxMessageSize() {
        return MAX_MESSAGE_SIZE;
    }

    protected ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE_NATIVE) {
            return new NativePipeSynchronousWriter(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.PIPE_STREAMING) {
            return new StreamingPipeSynchronousWriter(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousWriter(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousWriter(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.BLOCKING_MAPPED) {
            return new BlockingMappedSynchronousWriter(file, getMaxMessageSize());
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    protected IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> newSerdeHandlerFactory(
            final IAsynchronousHandlerFactory<FDate, FDate> handler) {
        final ISerde<FDate> serde = newSerde();
        return new SerdeAsynchronousHandlerFactory<FDate, FDate>(handler, serde, serde, MAX_MESSAGE_SIZE);
    }

    public static ISynchronousReader<FDate> newSerdeReader(final ISynchronousReader<IByteBufferProvider> reader) {
        final ISerde<FDate> serde = newSerde();
        final SerdeSynchronousReader<FDate> commandReader = new SerdeSynchronousReader<FDate>(reader, serde);
        return commandReader;
    }

    public static ISynchronousWriter<FDate> newSerdeWriter(final ISynchronousWriter<IByteBufferProvider> writer) {
        final ISerde<FDate> serde = newSerde();
        final SerdeSynchronousWriter<FDate> commandWriter = new SerdeSynchronousWriter<FDate>(writer, serde,
                MAX_MESSAGE_SIZE);
        return commandWriter;
    }

    protected static ISerde<FDate> newSerde() {
        final ISerde<FDate> serde = FDateSerde.GET;
        if (SIMULATED_ADDITONAL_MESSAGE_SIZE > 0) {
            //random
            //            return new AddRandomBytesDelegateSerde<FDate>(serde, MIN_MESSAGE_SIZE, SIMULATED_ADDITONAL_MESSAGE_SIZE,
            //                    PseudoRandomGenerators.newPseudoRandom(0L));
            //zero
            //            return new AddDefinedBytesDelegateSerde<FDate>(serde, MIN_MESSAGE_SIZE, SIMULATED_ADDITONAL_MESSAGE_SIZE,
            //                    Bytes.ZERO);
            //unallocated
            return new AddUndefinedBytesDelegateSerde<FDate>(serde, MIN_MESSAGE_SIZE, SIMULATED_ADDITONAL_MESSAGE_SIZE);
        } else {
            return serde;
        }
    }

    public static void printProgress(final OutputStream log, final String action, final Instant start, final int count,
            final int maxCount) throws IOException {
        if (count < 0) {
            //skip on warmup messages
            return;
        }
        //uncomment this to prevent the progress logging from delaying anything
        //        if (LATENCY_REPORT_FACTORY.isMeasuringLatency()) {
        //            return;
        //        }
        final Duration duration = start.toDuration();
        log.write(
                TextDescription
                        .format("%s: %s/%s (%s) %s during %s\n", action, count, maxCount,
                                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                                new ProcessedEventsRateString(count, duration), duration)
                        .getBytes());
    }

    public static LoopInterruptedCheck newLoopInterruptedCheck() {
        return new LoopInterruptedCheck(Duration.ONE_SECOND) {
            @Override
            protected boolean onInterval() throws InterruptedException {
                return true;
            }
        };
    }

}
