package com.coderworld968;


import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.hash.Funnels;
import java.util.stream.IntStream;
import org.junit.Test;

public class CWBloomFilterUnitTest {

    @Test
    public void givenCWBloomFilter_whenAddNStringsToIt_thenShouldNotReturnAnyFalsePositive() {
        //when
        CWBloomFilter<Integer> filter = CWBloomFilter.create(
                Funnels.integerFunnel(),
                500,
                0.01);

        //when
        filter.put(1);
        filter.put(2);
        filter.put(3);

        //then
        // the probability that it returns true, but is actually false is 1%
        assertThat(filter.mightContain(1)).isTrue();
        assertThat(filter.mightContain(2)).isTrue();
        assertThat(filter.mightContain(3)).isTrue();

        assertThat(filter.mightContain(100)).isFalse();
    }

    @Test
    public void givenCWBloomFilter_whenAddNStringsToItMoreThanDefinedExpectedInsertions_thenItWillReturnTrueForAlmostAllElements() {
        //when
        CWBloomFilter<Integer> filter = CWBloomFilter.create(
                Funnels.integerFunnel(),
                5,
                0.01);

        //when
        IntStream.range(0, 100_000).forEach(filter::put);


        //then
        assertThat(filter.mightContain(1)).isTrue();
        assertThat(filter.mightContain(2)).isTrue();
        assertThat(filter.mightContain(3)).isTrue();
        assertThat(filter.mightContain(1_000_000)).isTrue();
    }
}
