package de.invesdwin.context.integration.jppf.internal;

import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.IOUtils;
import org.jppf.serialization.JPPFSerialization;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.simpleapi.DefaultCoder;

import de.invesdwin.context.log.error.Err;

/**
 * http://www.jppf.org/doc/5.2/index.php?title=Specifying_alternate_serialization_schemes
 */
@Immutable
public class RemoteFastJPPFSerialization implements JPPFSerialization {

    static {
        // https://github.com/RuedigerMoeller/fast-serialization/issues/234
        FSTClazzInfo.BufferConstructorMeta = false;
        FSTClazzInfo.BufferFieldMeta = false;
    }

    private static final ThreadLocal<DefaultCoder> CONF_THREADLOCAL = new ThreadLocal<DefaultCoder>() {
        @Override
        protected DefaultCoder initialValue() {
            return new DefaultCoder();
        }
    };

    public RemoteFastJPPFSerialization() {}

    @Override
    public void serialize(final Object o, final OutputStream os) throws Exception {
        try {
            final DefaultCoder coder = CONF_THREADLOCAL.get();
            final byte[] bytes = coder.toByteArray(o);
            IOUtils.write(bytes, os);
        } catch (final Throwable t) {
            throw Err.process(t);
        }
    }

    @Override
    public Object deserialize(final InputStream is) throws Exception {
        try {
            final DefaultCoder coder = CONF_THREADLOCAL.get();
            final FSTConfiguration conf = coder.getConf();
            final ClassLoader previousClassLoader = conf.getClassLoader();
            try {
                final byte[] bytes = IOUtils.toByteArray(is);
                conf.setClassLoader(Thread.currentThread().getContextClassLoader());
                return coder.toObject(bytes);
            } finally {
                conf.setClassLoader(previousClassLoader);
            }
        } catch (final Throwable t) {
            throw Err.process(t);
        }
    }

}