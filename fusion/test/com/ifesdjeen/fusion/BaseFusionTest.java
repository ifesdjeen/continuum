package com.ifesdjeen.fusion;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public abstract class BaseFusionTest {

    protected final List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    public abstract Fusion<Integer, Integer> getFusion();

    @Test
    public void testTake() throws ExecutionException, InterruptedException {
        Integer res = getFusion()
                        .take(5)
                        .filter((i) -> i % 2 == 0)
                        .map((i) -> i + 1)
                        .fold(0, (acc, i) -> acc + i).get();
        assertThat(res, is(3 + 5));
    }

    public static class ListFusionTest extends BaseFusionTest {

        @Override
        public Fusion<Integer, Integer> getFusion() {
            return Fusion.from(input);
        }

    }

}
