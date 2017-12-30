package de.invesdwin.context.integration.streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream.BlockSize;

@Immutable
public final class LZ4Streams {

    public static final BlockSize LARGE_BLOCK_SIZE = BlockSize.M4;
    /*
     * 64KB is default in LZ4OutputStream (1 << 16) though 128K is almost the same speed with a bit better compression
     * on fast compressor
     * 
     * http://java-performance.info/performance-general-compression/
     */
    public static final BlockSize DEFAULT_BLOCK_SIZE = BlockSize.K64;

    private LZ4Streams() {}

    public static FramedLZ4CompressorOutputStream newDefaultLZ4OutputStream(final OutputStream out) {
        return newHighLZ4OutputStream(out, DEFAULT_BLOCK_SIZE);
    }

    public static FramedLZ4CompressorOutputStream newLargeLZ4OutputStream(final OutputStream out) {
        return newHighLZ4OutputStream(out, LARGE_BLOCK_SIZE);
    }

    public static FramedLZ4CompressorOutputStream newHighLZ4OutputStream(final OutputStream out,
            final BlockSize blockSize) {
        try {
            return new FramedLZ4CompressorOutputStream(out, new FramedLZ4CompressorOutputStream.Parameters(blockSize,
                    BlockLZ4CompressorOutputStream.createParameterBuilder().tunedForCompressionRatio().build()));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static FramedLZ4CompressorOutputStream newFastLZ4OutputStream(final OutputStream out) {
        return newFastLZ4OutputStream(out, DEFAULT_BLOCK_SIZE);
    }

    public static FramedLZ4CompressorOutputStream newFastLZ4OutputStream(final OutputStream out,
            final BlockSize blockSize) {
        try {
            return new FramedLZ4CompressorOutputStream(out, new FramedLZ4CompressorOutputStream.Parameters(blockSize,
                    BlockLZ4CompressorOutputStream.createParameterBuilder().tunedForSpeed().build()));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static FramedLZ4CompressorInputStream newDefaultLZ4InputStream(final InputStream in) {
        try {
            return new FramedLZ4CompressorInputStream(in);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
