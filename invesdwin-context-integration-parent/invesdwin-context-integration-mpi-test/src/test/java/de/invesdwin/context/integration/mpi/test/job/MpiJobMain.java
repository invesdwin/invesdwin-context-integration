package de.invesdwin.context.integration.mpi.test.job;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.zeroturnaround.exec.stream.slf4j.Slf4jOutputStream;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.platform.util.AspectJWeaverIncludesConfigurer;
import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.AChannelTest.ClientTask;
import de.invesdwin.context.integration.channel.AChannelTest.ServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWaitPool;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWaitPool;
import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.ProvidedMpiAdapter;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.streams.BroadcastingOutputStream;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class MpiJobMain extends AMain {

    private static final boolean BOOTSTRAP = true;
    private static final IMpiAdapter MPI;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
        MPI = ProvidedMpiAdapter.getProvidedInstance();
    }

    @Option(help = true, name = "-l", aliases = "--logDir", usage = "Defines the log directory")
    protected File logDir;

    private final Log log = new Log(this);

    public MpiJobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        test();
    }

    private void test() {
        final int size = MPI.size();
        Assertions.checkEquals(2, size);
        final int rank = MPI.rank();
        Assertions.assertThat(rank).isBetween(0, 1);
        testBcast();
        testBarrier();
        testPerformance();
    }

    private void testBarrier() {
        final Instant start = new Instant();
        log.info("%s/%s: Started waiting for barrier", MPI.rank() + 1, MPI.size());
        MPI.barrier();
        log.info("%s/%s: Finished waiting for barrier after %s", MPI.rank() + 1, MPI.size(), start);
    }

    private void testBcast() {
        final Instant start = new Instant();
        log.info("%s/%s: Started broadcast", MPI.rank() + 1, MPI.size());
        final int bcastValue = Integer.MAX_VALUE;
        switch (MPI.rank()) {
        case 0:
            try (ISynchronousWriter<IByteBufferProvider> bcastWriter = MPI.newBcastWriter(0,
                    AChannelTest.MAX_MESSAGE_SIZE)) {
                bcastWriter.open();
                final SynchronousWriterSpinWait<IByteBufferProvider> bcastWriterSpinWait = SynchronousWriterSpinWaitPool
                        .borrowObject(bcastWriter);
                try (ICloseableByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject()) {
                    buffer.putInt(0, bcastValue);
                    bcastWriterSpinWait.waitForWrite(buffer.slice(0, Integer.BYTES),
                            ContextProperties.DEFAULT_NETWORK_TIMEOUT);
                } finally {
                    SynchronousWriterSpinWaitPool.returnObject(bcastWriterSpinWait);
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            break;
        case 1:
            try (ISynchronousReader<IByteBufferProvider> bcastReader = MPI.newBcastReader(0,
                    AChannelTest.MAX_MESSAGE_SIZE)) {
                bcastReader.open();
                final SynchronousReaderSpinWait<IByteBufferProvider> bcastReaderSpinWait = SynchronousReaderSpinWaitPool
                        .borrowObject(bcastReader);
                try {
                    final IByteBuffer message = bcastReaderSpinWait
                            .waitForRead(ContextProperties.DEFAULT_NETWORK_TIMEOUT)
                            .asBuffer();
                    Assertions.checkEquals(Integer.BYTES, message.capacity());
                    Assertions.checkEquals(bcastValue, message.getInt(0));
                    bcastReaderSpinWait.getReader().readFinished();
                } finally {
                    SynchronousReaderSpinWaitPool.returnObject(bcastReaderSpinWait);
                }
            } catch (final IOException e1) {
                throw new RuntimeException(e1);
            }
            break;
        default:
            throw UnknownArgumentException.newInstance(int.class, MPI.rank());
        }
        log.info("%s/%s: Finished broadcast after %s", MPI.rank() + 1, MPI.size(), start);
    }

    private void testPerformance() {
        switch (MPI.rank()) {
        case 0:
            final ISynchronousWriter<FDate> requestWriter = AChannelTest
                    .newCommandWriter(MPI.newSendWriter(1, 0, AChannelTest.MAX_MESSAGE_SIZE));
            final ISynchronousReader<FDate> responseReader = AChannelTest
                    .newCommandReader(MPI.newRecvReader(MPI.anySource(), MPI.anyTag(), AChannelTest.MAX_MESSAGE_SIZE));
            try (OutputStream log = newLog(MPI.rank(), MPI.size(), ClientTask.class)) {
                new ClientTask(log, requestWriter, responseReader).run();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            break;
        case 1:
            final ISynchronousReader<FDate> requestReader = AChannelTest
                    .newCommandReader(MPI.newRecvReader(MPI.anySource(), MPI.anyTag(), AChannelTest.MAX_MESSAGE_SIZE));
            final ISynchronousWriter<FDate> responseWriter = AChannelTest
                    .newCommandWriter(MPI.newSendWriter(0, 0, AChannelTest.MAX_MESSAGE_SIZE));
            try (OutputStream log = newLog(MPI.rank(), MPI.size(), ServerTask.class)) {
                new ServerTask(log, requestReader, responseWriter).run();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            break;
        default:
            throw UnknownArgumentException.newInstance(int.class, MPI.rank());
        }
    }

    private OutputStream newLog(final int rank, final int size, final Class<?> taskClass) throws FileNotFoundException {
        final Slf4jOutputStream log = Slf4jStream.of(taskClass).asInfo();
        if (logDir == null) {
            return log;
        }
        final BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(
                new File(logDir, (rank + 1) + "_" + size + "_" + taskClass.getSimpleName() + ".log")));
        return new BroadcastingOutputStream(log, file);
    }

    public static void main(final String[] args) {
        final String[] jobArgs = MPI.init(args);
        try {
            new MpiJobMain(jobArgs).run();
        } catch (final Throwable t) {
            Err.process(t);
            MPI.abort(-1);
        } finally {
            MPI.free();
        }
        //kill any outstanding threads
        System.exit(0);
    }

}
