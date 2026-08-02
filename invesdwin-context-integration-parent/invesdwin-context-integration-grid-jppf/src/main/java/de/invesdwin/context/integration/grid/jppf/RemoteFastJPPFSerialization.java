package de.invesdwin.context.integration.grid.jppf;

import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.IOUtils;
import org.jppf.serialization.JPPFSerialization;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.simpleapi.DefaultCoder;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.time.date.FDate;
import io.netty.util.concurrent.FastThreadLocal;

/**
 * http://www.jppf.org/doc/5.2/index.php?title=Specifying_alternate_serialization_schemes
 * 
 * WARNING: this causes e.g. the JMX CONNECT response to miss the connectionId. Also DefaultJPPFSerialization suffers
 * from class loader issues on more recent JVMs. Thus DefaultJavaSerialization seems to be the best option right now.
 */
@Immutable
@Deprecated
public class RemoteFastJPPFSerialization implements JPPFSerialization {

    static {
        // https://github.com/RuedigerMoeller/fast-serialization/issues/234
        FSTClazzInfo.BufferConstructorMeta = false;
        FSTClazzInfo.BufferFieldMeta = false;
    }

    private static final FastThreadLocal<RefreshingCoderReference> CONF_THREADLOCAL = new FastThreadLocal<RefreshingCoderReference>() {
        @Override
        protected RefreshingCoderReference initialValue() {
            return new RefreshingCoderReference();
        }
    };

    private static volatile FDate lastRefreshTrigger = FDate.now();

    @Deprecated
    public RemoteFastJPPFSerialization() {}

    @Deprecated
    public static void refresh() {
        lastRefreshTrigger = FDate.now();
    }

    @Deprecated
    @Override
    public void serialize(final Object o, final OutputStream os) throws Exception {
        try {
            final DefaultCoder coder = CONF_THREADLOCAL.get().get();
            final byte[] bytes = coder.toByteArray(o);
            IOUtils.write(bytes, os);
        } catch (final Throwable t) {
            throw Err.process(t);
        }
    }

    @Deprecated
    @Override
    public Object deserialize(final InputStream is) throws Exception {
        try {
            final DefaultCoder coder = CONF_THREADLOCAL.get().get();
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

    private static final class RefreshingCoderReference {

        private DefaultCoder coder = new DefaultCoder();
        private FDate lastRefresh = FDate.now();

        public DefaultCoder get() {
            if (lastRefresh.isBeforeNotNullSafe(lastRefreshTrigger)) {
                coder = new DefaultCoder();
                lastRefresh = FDate.now();
            }
            return coder;
        }
    }

}