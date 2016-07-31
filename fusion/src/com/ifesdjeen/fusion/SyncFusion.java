package com.ifesdjeen.fusion;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class SyncFusion<INIT, FROM> extends Fusion<INIT, FROM>{

  protected SyncFusion(
    List<Function<Consumer, Consumer>> suppliers) {
    super(suppliers);
  }

  @SuppressWarnings("unchecked")
  public abstract <ACC> ACC fold(ACC init, BiFunction<ACC, FROM, ACC> fold);
}
