/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.core.task;

/*
public class FiberTest {

  @Test
  public void fiberTest() throws ExecutionException, InterruptedException {
    final FiberScheduler scheduler = new FiberForkJoinScheduler("test", 1, null, false);

    final Fiber fiber1 = new Fiber(scheduler, new SuspendableRunnable() {
      @Override
      public void run() throws SuspendExecution, InterruptedException {
        Fiber.sleep(4000);
        System.out.println("Fiber1 finished");
      }
    }).start();

    final Fiber fiber2 = new Fiber(scheduler, new SuspendableRunnable() {
      @Override
      public void run() throws SuspendExecution, InterruptedException {
        Fiber.sleep(3000);
        System.out.println("Fiber2 finished");
      }
    }).start();

    final Fiber fiber3 = new Fiber(scheduler, new SuspendableRunnable() {
      @Override
      public void run() throws SuspendExecution, InterruptedException {
        Fiber.sleep(3000);
        System.out.println("Fiber3 finished");
      }
    }).start();

    final Fiber fiber4 = new Fiber(scheduler, new SuspendableRunnable() {
      @Override
      public void run() throws SuspendExecution, InterruptedException {
        Fiber.sleep(2000);
        System.out.println("Fiber4 finished");
      }
    }).start();

    fiber1.join();
    fiber2.join();
    fiber3.join();
    fiber4.join();
  }

  @Test
  public void createManyFibersTest() throws ExecutionException, InterruptedException {
    final Set<Fiber> fibers = new HashSet<>();
    final FiberScheduler scheduler = new FiberForkJoinScheduler("test", 3, null, false);

    for (int i = 0; i < 10000; i++) {
      final Channel<Integer> ch = Channels.newChannel(5);

      final Fiber fiber1 = new Fiber(scheduler, new SuspendableRunnable() {
        @Override
        public void run() throws SuspendExecution {
          try {
            while (!Fiber.interrupted()) {
              final int i = ch.receive();
              System.out.println("Take " + i);
            }
          } catch (final InterruptedException e) {

          }
        }
      }).start();

      fibers.add(fiber1);
    }

    for (final Fiber fiber : fibers) {
      fiber.interrupt();
    }

    for (final Fiber fiber : fibers) {
      fiber.join();
    }
  }

  @Test
  public void fiberSleepAndWakeTest() throws ExecutionException, InterruptedException, SuspendExecution {
    final Channel<Integer> ch = Channels.newChannel(5);

    final FiberScheduler scheduler = new FiberForkJoinScheduler("test", 3, null, false);

    final Fiber fiber1 = new Fiber(scheduler, new SuspendableRunnable() {
      @Override
      public void run() throws SuspendExecution, InterruptedException {
        while (!Fiber.interrupted()) {
          final int i = ch.receive();
          System.out.println("Take " + i);
        }
      }
    }).start();

    for (int i = 0; i < 20; i++) {
      ch.send(i);
      Thread.sleep(10);
    }

    fiber1.join();
  }
}
*/
