package com.ifesdjeen.fusion.monoid;

public class SumMonoid implements Monoid<Integer> {
  @Override
  public Integer mappend(Integer left, Integer right) {
    return left + right;
  }

  @Override
  public Integer mempty() {
    return 0;
  }
}
