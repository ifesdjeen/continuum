package com.ifesdjeen.fusion;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

class ListFusion<INIT, FROM> extends SyncFusion<INIT, FROM> {

  private final List<INIT> list;

  public ListFusion(List<INIT> list) {
    super(new ArrayList<>());
    this.list = list;
  }

  private ListFusion(List<Function<Consumer, Consumer>> suppliers, List<INIT> list) {
    super(suppliers);
    this.list = list;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <TO> Fusion<INIT, TO> downstream(Function<Consumer<TO>, Consumer<FROM>> constructor) {
    suppliers.add((Function<Consumer, Consumer>) (Function) constructor);
    return new ListFusion<INIT, TO>(suppliers, list);
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
    int size = list.size();
    for (int i = 0; i < size; i++) {
      finalized.accept(list.get(i));
    }

    return finalizer.get();
  }

}
