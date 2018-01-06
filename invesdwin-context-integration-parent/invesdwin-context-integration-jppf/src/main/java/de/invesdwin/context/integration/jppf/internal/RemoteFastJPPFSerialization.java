package de.invesdwin.context.integration.jppf.internal;

import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.IOUtils;
import org.jppf.serialization.JPPFSerialization;
import org.nustaq.serialization.FSTConfiguration;

import de.invesdwin.context.log.error.Err;

/**
 * http://www.jppf.org/doc/5.2/index.php?title=Specifying_alternate_serialization_schemes
 */
@Immutable
public class RemoteFastJPPFSerialization implements JPPFSerialization {

    private final ThreadLocal<FSTConfiguration> confThreadLocal = new ThreadLocal<FSTConfiguration>() {
        @Override
        protected FSTConfiguration initialValue() {
            return FSTConfiguration.createDefaultConfiguration();
        }
    };

    public RemoteFastJPPFSerialization() {}

    @Override
    public void serialize(final Object o, final OutputStream os) throws Exception {
        try {
            final FSTConfiguration conf = confThreadLocal.get();
            final byte[] bytes = conf.asByteArray(o);
            IOUtils.write(bytes, os);
        } catch (final Throwable t) {
            throw Err.process(t);
        }
    }

    @Override
    public Object deserialize(final InputStream is) throws Exception {
        try {
            final FSTConfiguration conf = confThreadLocal.get();
            final ClassLoader previousClassLoader = conf.getClassLoader();
            try {
                final byte[] bytes = IOUtils.toByteArray(is);
                conf.setClassLoader(Thread.currentThread().getContextClassLoader());
                return conf.asObject(bytes);
            } finally {
                conf.setClassLoader(previousClassLoader);
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        }
    }
}