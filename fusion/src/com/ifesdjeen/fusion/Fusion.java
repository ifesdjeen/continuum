package com.ifesdjeen.fusion;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class Fusion<INIT, FROM> { // TODO: COVARIANT extends Fusion

  protected final List<Function<Consumer, Consumer>> suppliers;

  public static <T> SyncFusion<T, T> from(List<T> s) {
    return new ListFusion<>(s);
  }

  public static <T> SyncFusion<T, T> from(Iterator<T> s) {
    return new IteratorFusion<>(s);
  }

  public static <T> AsyncFusion<T, T> async() {
    return new AsyncFusion<>();
  }

  protected Fusion(List<Function<Consumer, Consumer>> suppliers) {
    this.suppliers = suppliers;
  }

  public <TO> Fusion<INIT, TO> map(Function<FROM, TO> fn) {
    return downstream(new Function<Consumer<TO>, Consumer<FROM>>() {
      @Override
      public Consumer<FROM> apply(Consumer<TO> downstream) {
        return (FROM i) -> {
          downstream.accept(fn.apply(i));
        };
      }
    });
  }

  public Fusion<INIT, FROM> filter(Predicate<FROM> pred) {
    return downstream((Consumer<FROM> downstream) -> {
      return (FROM i) -> {
        if (pred.test(i)) {
          downstream.accept(i);
        }
      };
    });
  }

  // allMatch(Predicate<? super T> predicate)
  // anyMatch(Predicate<? super T> predicate)
  // collect(Collector<? super T,A,R> collector)
  // concat(Stream<? extends T> a, Stream<? extends T> b)
  // count()
  // filter(Predicate<? super T> predicate)
  // findAny()
  // findFirst()
  // flatMap(Function<? super T,? extends Stream<? extends R>> mapper)
  // forEach(Consumer<? super T> action)
  // limit(long maxSize)
  // noneMatch(Predicate<? super T> predicate)
  // sorted()
  // sorted(Comparator<? super T> comparator)

  // fork

  //

  // distinct()
  //public abstract Fusion<INIT, FROM> take(int howMany);

  // TODO: implemen take in terms of shortened fusion
  //   {
  //    return downstream((downstream) -> {
  //      AtomicInteger countdown = new AtomicInteger(howMany);
  //      return (i) -> {
  //        downstream.accept(i);
  //        int more = countdown.decrementAndGet();
  //        if (more == 0) {
  //          doBreak.set(true);
  //        }
  //      };
  //
  //    });
  //  }

  @SuppressWarnings("unchecked")
  protected abstract <TO> Fusion<INIT, TO> downstream(Function<Consumer<TO>, Consumer<FROM>> constructor);

}
