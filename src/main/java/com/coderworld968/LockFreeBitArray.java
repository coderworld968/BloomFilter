package com.coderworld968;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class  LockFreeBitArray {
    private static final int LONG_ADDRESSABLE_BITS = 6;
    final AtomicLongArray data;
    private final AtomicLong bitCount;

    LockFreeBitArray(long bits) {
        this(new long[Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING))]);
    }

    // Used by serialization
    LockFreeBitArray(long[] data) {
        checkArgument(data.length > 0, "data length is zero!");
        this.data = new AtomicLongArray(data);
        this.bitCount = new AtomicLong(0);
        long bitCount = 0;
        for (long value : data) {
            bitCount += Long.bitCount(value);
        }
        this.bitCount.addAndGet(bitCount);
    }

    /** Returns true if the bit changed value. */
    boolean set(long bitIndex) {
        if (get(bitIndex)) {
            return false;
        }

        int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS);
        long mask = 1L << bitIndex; // only cares about low 6 bits of bitIndex

        long oldValue;
        long newValue;
        do {
            oldValue = data.get(longIndex);
            newValue = oldValue | mask;
            if (oldValue == newValue) {
                return false;
            }
        } while (!data.compareAndSet(longIndex, oldValue, newValue));

        // We turned the bit on, so increment bitCount.
        bitCount.addAndGet(1);
        return true;
    }

    boolean get(long bitIndex) {
        return (data.get((int) (bitIndex >>> 6)) & (1L << bitIndex)) != 0;
    }

    /**
     * Careful here: if threads are mutating the atomicLongArray while this method is executing, the
     * final long[] will be a "rolling snapshot" of the state of the bit array. This is usually good
     * enough, but should be kept in mind.
     */
    public static long[] toPlainArray(AtomicLongArray atomicLongArray) {
        long[] array = new long[atomicLongArray.length()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = atomicLongArray.get(i);
        }
        return array;
    }

    /** Number of bits */
    long bitSize() {
        return (long) data.length() * Long.SIZE;
    }

    /**
     * Number of set bits (1s).
     *
     * <p>Note that because of concurrent set calls and uses of atomics, this bitCount is a (very)
     * close *estimate* of the actual number of bits set. It's not possible to do better than an
     * estimate without locking. Note that the number, if not exactly accurate, is *always*
     * underestimating, never overestimating.
     */
    long bitCount() {
        return bitCount.get();
    }

    LockFreeBitArray copy() {
        return new LockFreeBitArray(toPlainArray(data));
    }

    /**
     * Combines the two BitArrays using bitwise OR.
     *
     * <p>NOTE: Because of the use of atomics, if the other LockFreeBitArray is being mutated while
     * this operation is executing, not all of those new 1's may be set in the final state of this
     * LockFreeBitArray. The ONLY guarantee provided is that all the bits that were set in the other
     * LockFreeBitArray at the start of this method will be set in this LockFreeBitArray at the end
     * of this method.
     */
    void putAll(LockFreeBitArray other) {
        checkArgument(
                data.length() == other.data.length(),
                "BitArrays must be of equal length (%s != %s)",
                data.length(),
                other.data.length());
        for (int i = 0; i < data.length(); i++) {
            long otherLong = other.data.get(i);

            long ourLongOld;
            long ourLongNew;
            boolean changedAnyBits = true;
            do {
                ourLongOld = data.get(i);
                ourLongNew = ourLongOld | otherLong;
                if (ourLongOld == ourLongNew) {
                    changedAnyBits = false;
                    break;
                }
            } while (!data.compareAndSet(i, ourLongOld, ourLongNew));

            if (changedAnyBits) {
                int bitsAdded = Long.bitCount(ourLongNew) - Long.bitCount(ourLongOld);
                bitCount.addAndGet(bitsAdded);
            }
        }
    }

    @Override
    public boolean equals(@NullableDecl Object o) {
        if (o instanceof LockFreeBitArray) {
            LockFreeBitArray lockFreeBitArray = (LockFreeBitArray) o;
            // TODO(lowasser): avoid allocation here
            return Arrays.equals(toPlainArray(data), toPlainArray(lockFreeBitArray.data));
        }
        return false;
    }

    @Override
    public int hashCode() {
        // TODO(lowasser): avoid allocation here
        return Arrays.hashCode(toPlainArray(data));
    }
}