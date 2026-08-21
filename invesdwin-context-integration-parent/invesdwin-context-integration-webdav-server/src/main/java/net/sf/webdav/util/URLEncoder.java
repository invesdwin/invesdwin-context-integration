package net.sf.webdav.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.BitSet;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.error.Throwables;

/**
 * 
 * This class is very similar to the java.net.URLEncoder class.
 * 
 * Unfortunately, with java.net.URLEncoder there is no way to specify to the java.net.URLEncoder which characters should
 * NOT be encoded.
 * 
 * This code was moved from DefaultServlet.java
 * 
 * @author Craig R. McClanahan
 * @author Remy Maucherat
 */
@NotThreadSafe
public class URLEncoder {

    protected static final char[] HEXADECIMAL = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
            'E', 'F' };
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(URLEncoder.class);

    // Array containing the safe characters set.
    protected BitSet safeCharacters = new BitSet(256);

    public URLEncoder() {
        for (char i = 'a'; i <= 'z'; i++) {
            addSafeCharacter(i);
        }
        for (char i = 'A'; i <= 'Z'; i++) {
            addSafeCharacter(i);
        }
        for (char i = '0'; i <= '9'; i++) {
            addSafeCharacter(i);
        }
    }

    public void addSafeCharacter(final char c) {
        safeCharacters.set(c);
    }

    public String encode(final String path) {
        final int maxBytesPerChar = 10;
        // int caseDiff = ('a' - 'A');
        final StringBuilder rewrittenPath = new StringBuilder(path.length());
        final java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(maxBytesPerChar);
        OutputStreamWriter writer = null;
        try {
            writer = new OutputStreamWriter(buf, "UTF8");
        } catch (final Exception e) {
            //CHECKSTYLE:OFF
            LOG.warn("{}", Throwables.getFullStackTrace(e));
            //CHECKSTYLE:ON
            writer = new OutputStreamWriter(buf);
        }

        for (int i = 0; i < path.length(); i++) {
            final int c = path.charAt(i);
            if (safeCharacters.get(c)) {
                rewrittenPath.append((char) c);
            } else {
                // convert to external encoding before hex conversion
                try {
                    writer.write((char) c);
                    writer.flush();
                } catch (final IOException e) {
                    buf.reset();
                    continue;
                }
                final byte[] ba = buf.toByteArray();
                for (int j = 0; j < ba.length; j++) {
                    // Converting each byte in the buffer
                    final byte toEncode = ba[j];
                    rewrittenPath.append('%');
                    final int low = toEncode & 0x0f;
                    final int high = (toEncode & 0xf0) >> 4;
                    rewrittenPath.append(HEXADECIMAL[high]);
                    rewrittenPath.append(HEXADECIMAL[low]);
                }
                buf.reset();
            }
        }
        return rewrittenPath.toString();
    }
}
