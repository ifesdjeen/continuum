package com.ifesdjeen.fusion;

import org.jctools.queues.MpscArrayQueue;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

// TODO: hide the <FROM> parameter in the inner class or/and
// simply expose the publisher with `notify`
public class AsyncFusion<INIT, FROM> extends Fusion<INIT, FROM> {

  private final List<Consumer<INIT>> consumers;
  private final MpscArrayQueue<INIT> runQueue;

  public AsyncFusion() {
    this(new ArrayList<Function<Consumer, Consumer>>(),
         new ArrayList<Consumer<INIT>>());
  }

  protected AsyncFusion(List<Function<Consumer, Consumer>> suppliers,
                        List<Consumer<INIT>> consumers) {
    super(suppliers);
    this.consumers = consumers;
    this.runQueue = new MpscArrayQueue<>(1024);

    new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          INIT el = runQueue.poll();
          if (el != null) {
            for (Consumer<INIT> consumer : consumers) {
              consumer.accept(el);
            }
          }
        }
      }
    }).start();
  }

  @Override
  public <TO> AsyncFusion<INIT, TO> map(Function<FROM, TO> fn) {
    return (AsyncFusion<INIT, TO>) super.map(fn);
  }

  public AsyncFusion<INIT, FROM> filter(Predicate<FROM> pred) {
    return (AsyncFusion<INIT, FROM>) super.filter(pred);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <TO> AsyncFusion<INIT, TO> downstream(Function<Consumer<TO>, Consumer<FROM>> constructor) {
    suppliers.add((Function<Consumer, Consumer>) (Function) constructor);
    return new AsyncFusion<INIT, TO>(suppliers, consumers);
  }

  public void notify(INIT t) {
    this.runQueue.offer(t);
  }

  @SuppressWarnings("unchecked")
  public <ACC> void consume(Consumer<FROM> consumer) {
    Consumer stack = consumer;
    for (int i = suppliers.size() - 1; i >= 0; i--) {
      stack = suppliers.get(i).apply(stack);
    }

    Consumer<INIT> finalized = (Consumer<INIT>) stack;
    this.consumers.add(finalized);
  }

  @SuppressWarnings("unchecked")
  public <ACC> Future<ACC> fold(ACC init, BiFunction<ACC, FROM, ACC> fold) {
    throw new NotImplementedException();
  }
}
