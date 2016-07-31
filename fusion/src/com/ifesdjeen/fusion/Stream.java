package com.ifesdjeen.fusion;

import java.util.List;
import java.util.function.Consumer;

public interface Stream<T> {

  //  public Step next();

  public static <T> void fromList(List<T> input, Consumer<T> consumer) {
    for (T i : input) {
      consumer.accept(i);
    }
  }


}
