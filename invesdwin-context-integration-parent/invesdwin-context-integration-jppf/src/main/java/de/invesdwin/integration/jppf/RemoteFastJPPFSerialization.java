package de.invesdwin.integration.jppf;

import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.IOUtils;
import org.jppf.serialization.JPPFSerialization;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;

/**
 * http://www.jppf.org/doc/5.2/index.php?title=Specifying_alternate_serialization_schemes
 */
@Immutable
public class RemoteFastJPPFSerialization implements JPPFSerialization {

    private final FSTCoder coder = new DefaultCoder(true);

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