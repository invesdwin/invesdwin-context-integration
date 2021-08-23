package de.invesdwin.context.integration.channel.reference;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class ReferenceSynchronousWriter<M> implements ISynchronousWriter<M> {

    private IMutableReference<ISynchronousCommand<M>> reference;

    public ReferenceSynchronousWriter(final IMutableReference<ISynchronousCommand<M>> reference) {
        this.reference = reference;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        reference.set(EmptySynchronousCommand.getInstance());
        reference = DisabledReference.getInstance();
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        write(new ImmutableSynchronousCommand<M>(type, sequence, message));
    }

    @Override
    public void write(final ISynchronousCommand<M> message) throws IOException {
        reference.set(message);
    }

}
