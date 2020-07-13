package com.coderworld968;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;

enum CWBloomFilterStrategies implements CWBloomFilter.Strategy {
    /**
     * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by Adam Kirsch and
     * Michael Mitzenmacher. The paper argues that this trick doesn't significantly deteriorate the
     * performance of a Bloom filter (yet only needs two 32bit hash functions).
     */
    MURMUR128_MITZ_32() {
        @Override
        public <T> boolean put(
                T object, Funnel<? super T> funnel, int numHashFunctions, LockFreeBitArray bits) {
            long bitSize = bits.bitSize();
            long hash64 = Hashing.murmur3_128().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                bitsChanged |= bits.set(combinedHash % bitSize);
            }
            return bitsChanged;
        }

        @Override
        public <T> boolean mightContain(
                T object, Funnel<? super T> funnel, int numHashFunctions, LockFreeBitArray bits) {
            long bitSize = bits.bitSize();
            long hash64 = Hashing.murmur3_128().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!bits.get(combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    },
    /**
     * This strategy uses all 128 bits of {@link Hashing#murmur3_128} when hashing. It looks
     * different than the implementation in MURMUR128_MITZ_32 because we're avoiding the
     * multiplication in the loop and doing a (much simpler) += hash2. We're also changing the index
     * to a positive number by AND'ing with Long.MAX_VALUE instead of flipping the bits.
     */
    MURMUR128_MITZ_64() {
        @Override
        public <T> boolean put(
                T object, Funnel<? super T> funnel, int numHashFunctions, LockFreeBitArray bits) {
            long bitSize = bits.bitSize();
            byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        @Override
        public <T> boolean mightContain(
                T object, Funnel<? super T> funnel, int numHashFunctions, LockFreeBitArray bits) {
            long bitSize = bits.bitSize();
            byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }

        private /* static */ long lowerEight(byte[] bytes) {
            return Longs.fromBytes(
                    bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        }

        private /* static */ long upperEight(byte[] bytes) {
            return Longs.fromBytes(
                    bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9],
                    bytes[8]);
        }
    };

    /**
     * Models a lock-free array of bits.
     *
     * <p>We use this instead of java.util.BitSet because we need access to the array of longs and we
     * need compare-and-swap.
     */

}