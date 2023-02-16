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

import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.AChannelTest.ReaderTask;
import de.invesdwin.context.integration.channel.AChannelTest.WriterTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.ProvidedMpiAdapter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.streams.BroadcastingOutputStream;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class MpiJobMain extends AMain {

    private static final IMpiAdapter MPI = ProvidedMpiAdapter.getProvidedInstance();

    @Option(help = true, name = "-l", aliases = "--logDir", usage = "Defines the log directory")
    protected File logDir;

    public MpiJobMain(final String[] args) {
        super(args);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        final int rank = MPI.rank();
        final int size = MPI.size();
        Assertions.checkEquals(2, size);
        switch (rank) {
        case 0:
            final ISynchronousWriter<FDate> requestWriter = AChannelTest
                    .newCommandWriter(MPI.newSendWriter(1, 0, AChannelTest.MAX_MESSAGE_SIZE));
            final ISynchronousReader<FDate> responseReader = AChannelTest
                    .newCommandReader(MPI.newRecvReader(MPI.anySource(), MPI.anyTag(), AChannelTest.MAX_MESSAGE_SIZE));
            try (OutputStream log = newLog(rank, size, ReaderTask.class)) {
                new ReaderTask(log, requestWriter, responseReader).run();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            break;
        case 1:
            final ISynchronousReader<FDate> requestReader = AChannelTest
                    .newCommandReader(MPI.newRecvReader(MPI.anySource(), MPI.anyTag(), AChannelTest.MAX_MESSAGE_SIZE));
            final ISynchronousWriter<FDate> responseWriter = AChannelTest
                    .newCommandWriter(MPI.newSendWriter(0, 0, AChannelTest.MAX_MESSAGE_SIZE));
            try (OutputStream log = newLog(rank, size, WriterTask.class)) {
                new WriterTask(log, requestReader, responseWriter).run();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            break;
        default:
            throw UnknownArgumentException.newInstance(int.class, rank);
        }
    }

    private OutputStream newLog(final int rank, final int size, final Class<?> taskClass) throws FileNotFoundException {
        final Slf4jOutputStream log = Slf4jStream.of(taskClass).asInfo();
        final BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(
                new File(logDir, (rank + 1) + "_" + size + "_" + taskClass.getSimpleName() + ".log")));
        return new BroadcastingOutputStream(log, file);
    }

    public static void main(final String[] args) {
        final String[] jobArgs = MPI.init(args);
        try {
            new MpiJobMain(jobArgs);
        } catch (final Throwable t) {
            Err.process(t);
            MPI.abort(-1);
        } finally {
            MPI.finalizeMpi();
        }
    }

}
