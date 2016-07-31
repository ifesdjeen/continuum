package com.ifesdjeen.fusion;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class FusionTest {

  @Test
  public void testFilter() {
    AtomicReference<Integer> ref = new AtomicReference<>();

    Operation<Integer, Integer> filter = Operation.filter((i) -> i % 2 == 0,
                                                          ref::set);

    filter.accept(1);
    assertThat(ref.get(), nullValue());

    filter.accept(2);
    assertThat(ref.get(), is(2));

    filter.accept(3);
    assertThat(ref.get(), is(2));

    filter.accept(4);
    assertThat(ref.get(), is(4));
  }

  @Test
  public void testMap() {
    AtomicReference<String> ref = new AtomicReference<>();

    Operation<Integer, String> map = Operation.map(Object::toString,
                                                   ref::set);

    map.accept(1);
    assertThat(ref.get(), is("1"));

    map.accept(2);
    assertThat(ref.get(), is("2"));
  }

  @Test
  public void testFinalizer() {
//    Integer res =
//      Fusion.from(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5)).iterator())
//                        .filter((i) -> i % 2 == 0)
//                        .map((i) -> i + 1)
//                        .fold(0,
//                              (acc, i) -> acc + i);
//    assertThat(res, is(3 + 5));
  }

//  @Test
//  public void testTake() {
//    Integer res =
//      new Fusion<Integer, Integer>(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).iterator())
//        .take(5)
//        .filter((i) -> i % 2 == 0)
//        .map((i) -> i + 1)
//        .fold(0,
//              (acc, i) -> acc + i);
//    assertThat(res, is(3 + 5));
//  }


}


