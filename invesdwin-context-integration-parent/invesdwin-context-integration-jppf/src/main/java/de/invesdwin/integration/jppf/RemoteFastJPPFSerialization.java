package de.invesdwin.integration.jppf;

import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.IOUtils;
import org.jppf.serialization.JPPFSerialization;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTConfiguration.StreamCoderFactory;
import org.nustaq.serialization.FSTDecoder;
import org.nustaq.serialization.FSTEncoder;
import org.nustaq.serialization.coders.FSTStreamDecoder;
import org.nustaq.serialization.coders.FSTStreamEncoder;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;

/**
 * http://www.jppf.org/doc/5.2/index.php?title=Specifying_alternate_serialization_schemes
 */
@Immutable
public class RemoteFastJPPFSerialization implements JPPFSerialization {

    private final FSTCoder coder;

    public RemoteFastJPPFSerialization() {
        this.coder = new DefaultCoder(true);
        final FSTConfiguration conf = coder.getConf();
        conf.setStreamCoderFactory(new StreamCoderFactory() {

            private final ThreadLocal<?> input = new ThreadLocal<Object>();
            private final ThreadLocal<?> output = new ThreadLocal<Object>();

            @Override
            public FSTEncoder createStreamEncoder() {
                return new FSTStreamEncoder(conf);
            }

            @Override
            public FSTDecoder createStreamDecoder() {
                return new FSTStreamDecoder(conf) {
                    @Override
                    public Class<?> classForName(final String name) throws ClassNotFoundException {
                        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
                        if (cl == null) {
                            return super.classForName(name);
                        }
                        return Class.forName(name, false, cl);
                    }
                };
            }

            @Override
            public ThreadLocal<?> getInput() {
                return input;
            }

            @Override
            public ThreadLocal<?> getOutput() {
                return output;
            }
        });
    }

    @Override
    public void serialize(final Object o, final OutputStream os) throws Exception {
        final byte[] bytes = coder.toByteArray(o);
        IOUtils.write(bytes, os);
    }

    @Override
    public Object deserialize(final InputStream is) throws Exception {
        final byte[] bytes = IOUtils.toByteArray(is);
        return coder.toObject(bytes);
    }
}