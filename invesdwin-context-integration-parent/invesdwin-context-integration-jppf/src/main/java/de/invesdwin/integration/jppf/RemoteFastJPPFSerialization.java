package de.invesdwin.integration.jppf;

import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.IOUtils;
import org.jppf.serialization.JPPFSerialization;
import org.nustaq.serialization.FSTConfiguration;

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
    public synchronized void serialize(final Object o, final OutputStream os) throws Exception {
        final FSTConfiguration conf = confThreadLocal.get();
        final byte[] bytes = conf.asByteArray(o);
        IOUtils.write(bytes, os);
    }

    @Override
    public synchronized Object deserialize(final InputStream is) throws Exception {
        final FSTConfiguration conf = confThreadLocal.get();
        final ClassLoader previousClassLoader = conf.getClassLoader();
        try {
            final byte[] bytes = IOUtils.toByteArray(is);
            conf.setClassLoader(Thread.currentThread().getContextClassLoader());
            return conf.asObject(bytes);
        } finally {
            conf.setClassLoader(previousClassLoader);
        }
    }
}