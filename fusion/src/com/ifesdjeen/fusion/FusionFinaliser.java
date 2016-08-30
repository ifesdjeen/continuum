package com.ifesdjeen.fusion;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class FusionFinaliser<LAST, RES> implements Consumer<LAST>, Future<RES> {

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

  @Override
  public RES get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return false;
  }
}
