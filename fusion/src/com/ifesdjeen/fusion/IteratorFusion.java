package com.ifesdjeen.fusion;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

class IteratorFusion<INIT, FROM> extends SyncFusion<INIT, FROM> {

  private final Iterator<INIT> iterator;

  public IteratorFusion(Iterator<INIT> iterator) {
    super(new ArrayList<>());
    this.iterator = iterator;
  }

  private IteratorFusion(List<Function<Consumer, Consumer>> suppliers, Iterator<INIT> list) {
    super(suppliers);
    this.iterator = list;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <TO> Fusion<INIT, TO> downstream(Function<Consumer<TO>, Consumer<FROM>> constructor) {
    suppliers.add((Function<Consumer, Consumer>) (Function) constructor);
    return new IteratorFusion<INIT, TO>(suppliers, iterator);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <ACC> ACC fold(ACC init,
                        BiFunction<ACC, FROM, ACC> fold) {
    FusionFinaliser<FROM, ACC> finalizer = new FusionFinaliser<>(init,
                                                                 fold);
    Consumer stack = finalizer;
    for (int i = suppliers.size() - 1; i >= 0; i--) {
      stack = suppliers.get(i).apply(stack);
    }

    Consumer<INIT> finalized = (Consumer<INIT>) stack;
    while (iterator.hasNext()) {
      finalized.accept(iterator.next());
    }

    return finalizer.get();
  }

}
