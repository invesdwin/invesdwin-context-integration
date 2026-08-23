package de.invesdwin.context.integration.webdav;

import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.webdav.test.LocalWebdavFileChannelStub;
import de.invesdwin.context.integration.ws.registry.RegistryServiceStub;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContextSetup;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.math.random.PseudoRandomGenerator;
import jakarta.inject.Inject;

@NotThreadSafe
public class WebdavFileChannelTest extends ATest {

    @Inject
    private WebdavServerDestinationProvider destinationProvider;

    @Override
    public void setUpContext(final ITestContextSetup ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.deactivateBean(RegistryServiceStub.class);
        ctx.deactivateBean(LocalWebdavFileChannelStub.class);
    }

    @Test
    public void test() {
        final URI destination = getDestination();
        //CHECKSTYLE:OFF
        final WebdavFileChannel channel = new WebdavFileChannel(destination)
                .setSubDirectory(WebdavFileChannelTest.class.getSimpleName());
        //CHECKSTYLE:ON
        channel.setFilename("noexisting");
        channel.connect();
        Assertions.checkNull(channel.downloadBytes());
        Assertions.checkFalse(channel.exists());
        Assertions.assertThat(channel.length()).isEqualTo(-1);
        channel.createUniqueFile();
        Assertions.checkTrue(channel.exists());
        Assertions.assertThat(channel.length()).isEqualTo(0);
        final String writeStr = "hello world";
        final byte[] write = writeStr.getBytes();
        channel.upload(write);
        Assertions.checkTrue(channel.exists());
        Assertions.assertThat(channel.length()).isEqualTo(write.length);
        final byte[] read = channel.downloadBytes();
        final String readStr = new String(read);
        Assertions.assertThat(readStr).isEqualTo(writeStr);
        channel.delete();
        Assertions.checkNull(channel.downloadBytes());
        Assertions.checkFalse(channel.exists());
        Assertions.assertThat(channel.length()).isEqualTo(-1);
        channel.upload(write);
        Assertions.checkTrue(channel.exists());
        Assertions.assertThat(channel.length()).isEqualTo(write.length);
        final byte[] read2 = channel.downloadBytes();
        final String readStr2 = new String(read2);
        Assertions.assertThat(readStr2).isEqualTo(writeStr);
        channel.delete();
        channel.close();
    }

    protected URI getDestination() {
        return destinationProvider.getDestination();
    }

    @Test
    public void testRandom() {
        final URI destination = getDestination();
        //CHECKSTYLE:OFF
        final WebdavFileChannel channel = new WebdavFileChannel(destination)
                .setSubDirectory(WebdavFileChannelTest.class.getSimpleName());
        //CHECKSTYLE:ON
        channel.connect();
        channel.createUniqueFile();
        final String writeStr = "hello world";
        final byte[] write = writeStr.getBytes();

        final PseudoRandomGenerator randomGenerator = new PseudoRandomGenerator();
        for (int i = 0; i < 20; i++) {
            final int random = randomGenerator.nextInt(0, 7);
            switch (random) {
            case 0:
                log.info("downloadBytes");
                channel.downloadBytes();
                break;
            case 1:
                log.info("exists");
                channel.exists();
                break;
            case 2:
                log.info("length");
                channel.length();
                break;
            case 3:
                log.info("createUniqueFile");
                channel.createUniqueFile();
                break;
            case 4:
                log.info("upload");
                channel.upload(write);
                break;
            case 5:
                log.info("delete");
                channel.delete();
                break;
            case 6:
                log.info("lastModified");
                channel.lastModified();
                break;
            default:
                throw UnknownArgumentException.newInstance(int.class, random);
            }
        }

        channel.close();
    }

}
