package com.ifesdjeen.fusion;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class FusionFinaliser<LAST, RES> implements Consumer<LAST>, Supplier<RES> {

  private       RES                        ref;
  private final BiFunction<RES, LAST, RES> folder;

  public FusionFinaliser(RES init,
                         BiFunction<RES, LAST, RES> folder) {
    this.ref = init;
    this.folder = folder;
  }

  @Override
  public void accept(LAST t) {
    this.ref = folder.apply(ref, t);
  }

  @Override
  public RES get() {
    return ref;
  }
}
