package de.invesdwin.context.integration.mpi;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import jakarta.inject.Named;

/**
 * This instance will use the IMpiSynchronousChannelFactory that was chosen by the user either by including the
 * appropriate runtime module in the classpath or by defining the class to be used as a system property.
 */
@Immutable
@Named
public final class ProvidedMpiAdapter implements IMpiAdapter, FactoryBean<ProvidedMpiAdapter> {

    public static final String PROVIDED_INSTANCE_KEY = IMpiAdapter.class.getName();

    public static final ProvidedMpiAdapter INSTANCE = new ProvidedMpiAdapter();

    @GuardedBy("this.class")
    private static IMpiAdapter providedInstance;

    private ProvidedMpiAdapter() {}

    public static synchronized IMpiAdapter getProvidedInstance() {
        if (providedInstance == null) {
            final SystemProperties systemProperties = new SystemProperties();
            if (systemProperties.containsValue(PROVIDED_INSTANCE_KEY)) {
                try {
                    final String factory = systemProperties.getString(PROVIDED_INSTANCE_KEY);
                    return (IMpiAdapter) Reflections.classForName(factory).newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            } else {
                final Map<String, IMpiAdapter> factories = new LinkedHashMap<String, IMpiAdapter>();
                for (final IMpiAdapter factory : ServiceLoader.load(IMpiAdapter.class)) {
                    final IMpiAdapter existing = factories.put(factory.getClass().getName(), factory);
                    if (existing != null) {
                        throw new IllegalStateException("Duplicate service provider found for [" + PROVIDED_INSTANCE_KEY
                                + "=" + existing.getClass().getName()
                                + "]. Please make sure you have only one provider for it in the classpath.");
                    }
                }
                if (factories.isEmpty()) {
                    throw new IllegalStateException("No service provider found for [" + PROVIDED_INSTANCE_KEY
                            + "]. Please add one provider for it to the classpath.");
                }
                if (factories.size() > 1) {
                    final StringBuilder factoriesStr = new StringBuilder("(");
                    for (final String factory : factories.keySet()) {
                        factoriesStr.append(factory);
                        factoriesStr.append("|");
                    }
                    Strings.removeEnd(factoriesStr, "|");
                    factoriesStr.append(")");
                    throw new IllegalStateException("More than one service provider found for [" + PROVIDED_INSTANCE_KEY
                            + "=" + factoriesStr
                            + "] to choose from. Please remove unwanted ones from the classpath or choose a "
                            + "specific one by defining a system property for the preferred one. E.g. on the command line with -D"
                            + PROVIDED_INSTANCE_KEY + "=" + factories.keySet().iterator().next());
                }
                setProvidedInstance(factories.values().iterator().next());
            }
        }
        return providedInstance;
    }

    public static synchronized void setProvidedInstance(final IMpiAdapter providedInstance) {
        ProvidedMpiAdapter.providedInstance = providedInstance;
        final SystemProperties systemProperties = new SystemProperties();
        if (providedInstance == null) {
            systemProperties.setString(PROVIDED_INSTANCE_KEY, null);
        } else {
            systemProperties.setString(PROVIDED_INSTANCE_KEY, providedInstance.getClass().getName());
        }
    }

    @Override
    public void init(final String[] args) {
        getProvidedInstance().init(args);
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
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final int root) {
        return getProvidedInstance().newBcastWriter(root);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final IIntReference root) {
        return getProvidedInstance().newBcastWriter(root);
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
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final int dest, final int tag) {
        return getProvidedInstance().newSendWriter(dest, tag);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final IIntReference dest, final IIntReference tag) {
        return getProvidedInstance().newSendWriter(dest, tag);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final int source, final int tag,
            final int maxMessageSize) {
        return getProvidedInstance().newRecvReader(source, tag, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final IIntReference source, final IIntReference tag,
            final int maxMessageSize) {
        return getProvidedInstance().newRecvReader(source, tag, maxMessageSize);
    }

    @Override
    public void abort(final int errorCode) {
        getProvidedInstance().abort(errorCode);
    }

    @Override
    public void close() {
        getProvidedInstance().close();
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
