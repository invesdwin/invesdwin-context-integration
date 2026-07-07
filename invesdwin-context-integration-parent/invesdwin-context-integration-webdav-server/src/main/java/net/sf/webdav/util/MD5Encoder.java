package net.sf.webdav.util;

import javax.annotation.concurrent.Immutable;

/**
 * Encode an MD5 digest into a String.
 * <p>
 * The 128 bit MD5 hash is converted into a 32 character long String. Each character of the String is the hexadecimal
 * representation of 4 bits of the digest.
 * 
 * @author Remy Maucherat
 * @version $Revision: 1.2 $ $Date: 2008-08-05 07:38:45 $
 */

@Immutable
public final class MD5Encoder {

    // ----------------------------------------------------- Instance Variables

    private static final char[] HEXADECIMAL = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };

    // --------------------------------------------------------- Public Methods

    /**
     * Encodes the 128 bit (16 bytes) MD5 into a 32 character String.
     * 
     * @param binaryData
     *            Array containing the digest
     * @return Encoded MD5, or null if encoding failed
     */
    public String encode(final byte[] binaryData) {

        if (binaryData.length != 16) {
            return null;
        }

        final char[] buffer = new char[32];

        for (int i = 0; i < 16; i++) {
            final int low = binaryData[i] & 0x0f;
            final int high = (binaryData[i] & 0xf0) >> 4;
            buffer[i * 2] = HEXADECIMAL[high];
            buffer[i * 2 + 1] = HEXADECIMAL[low];
        }

        return new String(buffer);

    }

}
