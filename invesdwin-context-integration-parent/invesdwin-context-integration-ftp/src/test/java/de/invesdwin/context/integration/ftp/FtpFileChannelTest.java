package de.invesdwin.context.integration.ftp;

import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.junit.Test;

import de.invesdwin.context.integration.ws.registry.RegistryServiceStub;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class FtpFileChannelTest extends ATest {

    @Inject
    private FtpServerDestinationProvider destinationProvider;

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.deactivate(RegistryServiceStub.class);
    }

    @Test
    public void test() {
        final URI destination = destinationProvider.getDestination();
        final FtpFileChannel channel = new FtpFileChannel(destination, FtpFileChannelTest.class.getSimpleName());
        channel.setFilename("noexisting");
        channel.connect();
        Assertions.checkNull(channel.read());
        Assertions.checkFalse(channel.exists());
        Assertions.assertThat(channel.size()).isEqualTo(-1);
        channel.createUniqueFile();
        Assertions.checkTrue(channel.exists());
        Assertions.assertThat(channel.size()).isEqualTo(0);
        final String writeStr = "hello world";
        final byte[] write = writeStr.getBytes();
        channel.write(write);
        Assertions.checkTrue(channel.exists());
        Assertions.assertThat(channel.size()).isEqualTo(write.length);
        final byte[] read = channel.read();
        final String readStr = new String(read);
        Assertions.assertThat(readStr).isEqualTo(writeStr);
        channel.delete();
        Assertions.checkNull(channel.read());
        Assertions.checkFalse(channel.exists());
        Assertions.assertThat(channel.size()).isEqualTo(-1);
        channel.write(write);
        Assertions.checkTrue(channel.exists());
        Assertions.assertThat(channel.size()).isEqualTo(write.length);
        final byte[] read2 = channel.read();
        final String readStr2 = new String(read2);
        Assertions.assertThat(readStr2).isEqualTo(writeStr);
        channel.delete();
        channel.close();
    }

}
