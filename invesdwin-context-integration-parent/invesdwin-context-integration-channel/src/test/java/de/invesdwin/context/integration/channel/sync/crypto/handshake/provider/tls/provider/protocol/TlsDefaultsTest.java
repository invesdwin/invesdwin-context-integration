package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

@NotThreadSafe
public class TlsDefaultsTest {

    @Test
    public void test() {
        //CHECKSTYLE:OFF
        System.out.println(new TlsDefaults(TlsProtocol.DTLS).toStringMultiline());
        //CHECKSTYLE:ON
    }

}
