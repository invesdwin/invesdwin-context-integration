package de.invesdwin.context.integration.ftp;

import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.junit.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class FtpFileChannelTest extends ATest {

    @Inject
    private FtpServerDestinationProvider destinationProvider;

    @Test
    public void test() {
        final URI destination = destinationProvider.getDestination();
        final FtpFileChannel channel = new FtpFileChannel(destination, FtpFileChannelTest.class.getSimpleName());
        channel.connect();
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
        Assertions.checkFalse(channel.exists());
        Assertions.assertThat(channel.size()).isEqualTo(-1);
        channel.close();
    }

}
