package com.liquidlabs.common.compression;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.xerial.snappy.Snappy;

/*
 * See
 * https://github.com/jpountz/lz4-java (fastest and most universal)
 */
public class CompressorConfig {

    static final public Types selected = Types.SNAPPY;

    enum Types { QUICK_LZ, LZF, SNAPPY };

    static public byte[] compress(byte[] src) throws IOException {
        if (selected == Types.SNAPPY) {
            return Snappy.compress(src);
        } else {
            final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
            final LZ4Compressor compressor = lz4Factory.fastCompressor();
            final int maxCompressedLength = compressor.maxCompressedLength(src.length);
            final byte[] bytes = toByteArray(src.length);
            final byte[] dest = new byte[maxCompressedLength + bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                dest[i] = bytes[i];
            }
            compressor.compress(src, 0, src.length, dest, bytes.length);
            return dest;
        }
	}

	static public byte[] decompress(final byte[] src) throws IOException {

        if (selected == Types.SNAPPY) {
            return Snappy.uncompress(src);
        } else {
            final int destLength = fromByteArray(src);
            final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
            final byte[] dest = new byte[destLength];
            lz4Factory.fastDecompressor().decompress(src, 4, dest, 0, destLength);
            return dest;
        }
	}
    static byte[] toByteArray(int value) {
        return new byte[] {
                (byte)(value >> 24),
                (byte)(value >> 16),
                (byte)(value >> 8),
                (byte)value };
    }

    static int fromByteArray(byte[] bytes) {
        return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
    }}
