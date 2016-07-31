package com.ifesdjeen.fusion.monoid;

public interface Monoid<M> {

  public M mappend(M left, M right);
  public M mempty();

}
