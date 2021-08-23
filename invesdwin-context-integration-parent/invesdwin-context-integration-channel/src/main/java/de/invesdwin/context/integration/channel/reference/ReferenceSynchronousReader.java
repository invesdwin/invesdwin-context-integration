package de.invesdwin.context.integration.channel.reference;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class ReferenceSynchronousReader<M> implements ISynchronousReader<M> {

    private IMutableReference<ISynchronousCommand<M>> reference;

    public ReferenceSynchronousReader(final IMutableReference<ISynchronousCommand<M>> reference) {
        this.reference = reference;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        reference = DisabledReference.getInstance();
    }

    @Override
    public boolean hasNext() throws IOException {
        return reference.get() != null;
    }

    @Override
    public ISynchronousCommand<M> readMessage() throws IOException {
        final ISynchronousCommand<M> message = reference.getAndSet(null);
        if (message == EmptySynchronousCommand.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
