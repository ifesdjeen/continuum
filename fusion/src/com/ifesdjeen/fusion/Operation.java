package com.ifesdjeen.fusion;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface Operation<FROM, TO> extends Consumer<FROM> {

  public static <FROM, TO> Operation<FROM, TO> map(Function<FROM, TO> fn,
                                                   Consumer<TO> downstream) {
    return (a) -> {
      downstream.accept(fn.apply(a));
    };
  }

  public static <T> Operation<T, T> filter(Predicate<T> pred,
                                           Consumer<T> downstream) {
    return (a) -> {
      if (pred.test(a)) {
        downstream.accept(a);
      }
    };
  }

  // TODO: zip fusions
}
