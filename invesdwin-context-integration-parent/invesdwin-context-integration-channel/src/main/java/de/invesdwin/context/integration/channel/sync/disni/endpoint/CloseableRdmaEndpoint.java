package de.invesdwin.context.integration.channel.sync.disni.endpoint;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.verbs.RdmaCmId;

@NotThreadSafe
public class CloseableRdmaEndpoint extends RdmaEndpoint implements Closeable {

    protected CloseableRdmaEndpoint(final RdmaEndpointGroup<? extends RdmaEndpoint> group, final RdmaCmId idPriv,
            final boolean serverSide) throws IOException {
        super(group, idPriv, serverSide);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

}
