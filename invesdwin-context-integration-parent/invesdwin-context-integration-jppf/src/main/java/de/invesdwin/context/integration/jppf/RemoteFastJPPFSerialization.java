package de.invesdwin.context.integration.jppf;

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
 */
@Immutable
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

    private static volatile FDate lastRefreshTrigger = new FDate();

    public RemoteFastJPPFSerialization() {}

    public static void refresh() {
        lastRefreshTrigger = new FDate();
    }

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
        private FDate lastRefresh = new FDate();

        public DefaultCoder get() {
            if (lastRefresh.isBefore(lastRefreshTrigger)) {
                coder = new DefaultCoder();
                lastRefresh = new FDate();
            }
            return coder;
        }
    }

}