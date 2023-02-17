package de.invesdwin.context.integration.mpi;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.concurrent.reference.integer.IMutableIntReference;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import jakarta.inject.Named;

/**
 * This instance will use the IMpiAdapter that was chosen by the user either by including the appropriate runtime module
 * in the classpath as detected by ProvidedMpiAdapterFactory.
 */
@Immutable
@Named
public final class ProvidedMpiAdapter implements IMpiAdapter, FactoryBean<ProvidedMpiAdapter> {

    public static final ProvidedMpiAdapter INSTANCE = new ProvidedMpiAdapter();

    @GuardedBy("this.class")
    private static IMpiAdapter providedInstance;

    private ProvidedMpiAdapter() {}

    public static synchronized IMpiAdapter getProvidedInstance() {
        if (providedInstance == null) {
            providedInstance = ProvidedMpiAdapterFactory.INSTANCE.newInstance();
        }
        return providedInstance;
    }

    @Override
    public String[] init(final String[] args) {
        return getProvidedInstance().init(args);
    }

    @Override
    public MpiThreadSupport initThread(final String[] args, final MpiThreadSupport required) {
        return getProvidedInstance().initThread(args, required);
    }

    @Override
    public int rank() {
        return getProvidedInstance().rank();
    }

    @Override
    public int size() {
        return getProvidedInstance().size();
    }

    @Override
    public int anySource() {
        return getProvidedInstance().anySource();
    }

    @Override
    public int anyTag() {
        return getProvidedInstance().anyTag();
    }

    @Override
    public void barrier() {
        getProvidedInstance().barrier();
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final int root, final int maxMessageSize) {
        return getProvidedInstance().newBcastWriter(root, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final IIntReference root, final int maxMessageSize) {
        return getProvidedInstance().newBcastWriter(root, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newBcastReader(final int root, final int maxMessageSize) {
        return getProvidedInstance().newBcastReader(root, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newBcastReader(final IIntReference root, final int maxMessageSize) {
        return getProvidedInstance().newBcastReader(root, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final int dest, final int tag,
            final int maxMessageSize) {
        return getProvidedInstance().newSendWriter(dest, tag, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final IIntReference dest, final IIntReference tag,
            final int maxMessageSize) {
        return getProvidedInstance().newSendWriter(dest, tag, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final IMutableIntReference source,
            final IMutableIntReference tag, final int maxMessageSize) {
        return getProvidedInstance().newRecvReader(source, tag, maxMessageSize);
    }

    @Override
    public IMpiAdapter split(final int color, final int key) {
        return getProvidedInstance().split(color, key);
    }

    @Override
    public void abort(final int errorCode) {
        getProvidedInstance().abort(errorCode);
    }

    @Override
    public void free() {
        getProvidedInstance().free();
    }

    @Override
    public Class<?> getObjectType() {
        return ProvidedMpiAdapter.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public ProvidedMpiAdapter getObject() throws Exception {
        return INSTANCE;
    }

}
