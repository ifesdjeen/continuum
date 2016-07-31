package com.ifesdjeen.fusion.either;

public interface Either<E extends Throwable, T> {

    public boolean isLeft();
    public boolean isRight();

    public E left();
    public T right();

}
