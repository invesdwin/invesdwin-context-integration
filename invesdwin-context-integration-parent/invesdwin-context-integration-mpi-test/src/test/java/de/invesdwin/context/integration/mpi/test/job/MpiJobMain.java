package de.invesdwin.context.integration.mpi.test.job;

import javax.annotation.concurrent.NotThreadSafe;

import org.kohsuke.args4j.CmdLineParser;

import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.ProvidedMpiAdapter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class MpiJobMain extends AMain {

    private static final IMpiAdapter MPI = ProvidedMpiAdapter.getProvidedInstance();

    public MpiJobMain(final String[] args) {
        super(args);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) throws Exception {
        final int rank = MPI.rank();
        final int size = MPI.size();
        Assertions.checkEquals(2, size);
        switch (rank) {
        case 0:
            final ISynchronousWriter<FDate> requestWriter = AChannelTest
                    .newCommandWriter(MPI.newSendWriter(1, 0, AChannelTest.MAX_MESSAGE_SIZE));
            final ISynchronousReader<FDate> responseReader = AChannelTest
                    .newCommandReader(MPI.newRecvReader(MPI.anySource(), MPI.anyTag(), AChannelTest.MAX_MESSAGE_SIZE));
            new AChannelTest.ReaderTask(requestWriter, responseReader).run();
            break;
        case 1:
            final ISynchronousReader<FDate> requestReader = AChannelTest
                    .newCommandReader(MPI.newRecvReader(MPI.anySource(), MPI.anyTag(), AChannelTest.MAX_MESSAGE_SIZE));
            final ISynchronousWriter<FDate> responseWriter = AChannelTest
                    .newCommandWriter(MPI.newSendWriter(0, 0, AChannelTest.MAX_MESSAGE_SIZE));
            new AChannelTest.WriterTask(requestReader, responseWriter).run();
            break;
        default:
            throw UnknownArgumentException.newInstance(int.class, rank);
        }
    }

    public static void main(final String[] args) {
        MPI.init(args);
        try {
            new MpiJobMain(args);
        } catch (final Throwable t) {
            Err.process(t);
            MPI.abort(-1);
        } finally {
            MPI.close();
        }
    }

}
