package com.example.concurrency.features.spin;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author suizi
 */
public class CustomSpinLock {
}

/**
 * 自旋锁基本实现
 */
class SpinLock {

    // 0 表示未上锁状态
    // 1 表示上锁状态
    protected AtomicInteger value;

    public SpinLock() {
        this.value = new AtomicInteger();
        // 设置 value 的初始值为0 表示未上锁的状态
        this.value.set(0);
    }

    public void lock() {
        // 进行自旋操作
        while (!value.compareAndSet(0, 1)) ;
    }

    public void unlock() {
        // 将锁的状态设置为未上锁状态
        value.compareAndSet(1, 0);
    }
}

/**
 * 自旋锁验证
 */
class SpinLockTest {
    public static int data;
    public static SpinLock lock = new SpinLock();

    public static void add() {
        for (int i = 0; i < 100000; i++) {
            // 上锁 只能有一个线程执行 data++ 操作 其余线程都只能进行while循环
            lock.lock();
            data++;
            lock.unlock();
        }
    }

    /**
     * 可重入锁死锁验证
     *
     * @param state
     * @throws InterruptedException
     */
    public static void add(int state) throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        if (state <= 3) {
            lock.lock();
            System.out.println(Thread.currentThread().getName() + "\t进入临界区 state = " + state);
            for (int i = 0; i < 10; i++)
                data++;
            add(state + 1); // 进行递归重入 重入之前锁状态已经是1了 因为这个线程进入了临界区
            lock.unlock();
        }
    }


    public static void main(String[] args) throws InterruptedException {
       /* // 自旋锁验证
        Thread[] threads = new Thread[100];
        // 设置100个线程
        for (int i = 0; i < 100; i ++) {
            threads[i] = new Thread(SpinLockTest::add);
        }
        // 启动一百个线程
        for (int i = 0; i < 100; i++) {
            threads[i].start();
        }
        // 等待这100个线程执行完成
        for (int i = 0; i < 100; i++) {
            threads[i].join();
        }
        System.out.println(data); // 10000000*/

        // 可重入锁验证
        /**
         * 当中加入我们传入的参数state的值为1，那么在线程执行for循环之后再次递归调用add函数的话，那么state的值就变成了2。
         * if条件仍然满足，这个线程也需要重新获得锁，但是此时锁的状态是1，这个线程已经获得过一次锁了，但是自旋锁期待的锁的状态是0，因为只有这样他才能够再次获得锁，
         * 进入临界区，但是现在锁的状态是1，也就是说虽然这个线程获得过一次锁，但是它也会一直进行while循环而且永远都出不来了，这样就形成了死锁了。
         */
        SpinLockTest.add(0);
    }


}

/**
 * 可重入自旋锁基本实现
 */
class ReentrantSpinLock extends SpinLock {

    private Thread owner;
    private int count;

    @Override
    public void lock() {
        /**
         * null 0 0 0  null	Thread[Thread-1,5,main]	0	0
         * 0    0 1 1  Thread[Thread-1,5,main]	Thread[Thread-1,5,main]	1	1
         * 0    0 2 1  Thread[Thread-1,5,main]	Thread[Thread-1,5,main]	2	1
         *
         *
                 null	Thread[Thread-0,5,main]	0	0
                 Thread-0	进入临界区 state = 1
                 Thread[Thread-0,5,main]	Thread[Thread-0,5,main]	1	1
                 Thread-0	进入临界区 state = 2
                 Thread[Thread-0,5,main]	Thread[Thread-0,5,main]	2	1
                 Thread-0	进入临界区 state = 3
         */
        System.out.println(owner + "\t" + Thread.currentThread() + "\t" + count + "\t" + value);
        if (owner == null || owner != Thread.currentThread()) {
            while (!value.compareAndSet(0, 1)) ;
            owner = Thread.currentThread();
            count = 1;
        } else {
            count++;
        }

    }

    @Override
    public void unlock() {
        /**
         * Thread[Thread-0,5,main]	Thread[Thread-0,5,main]	3	1
         * Thread[Thread-0,5,main]	Thread[Thread-0,5,main]	2	1
         * Thread[Thread-0,5,main]	Thread[Thread-0,5,main]	1	1
         */
        System.out.println(owner + "\t" + Thread.currentThread() + "\t" + count + "\t" + value);
        if (count == 1) {
            count = 0;
            value.compareAndSet(1, 0);
        } else count--;
    }
}

class ReentrantSpinLockTest {

    public static int data;
    public static ReentrantSpinLock lock = new ReentrantSpinLock();

    public static void add(int state) throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        if (state <= 3) {
            lock.lock();
            System.out.println(Thread.currentThread().getName() + "\t进入临界区 state = " + state);
            for (int i = 0; i < 10; i++)
                data++;
            add(state + 1);
            lock.unlock();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(new Thread(() -> {
                try {
                    ReentrantSpinLockTest.add(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }, String.valueOf(i)));
        }
        for (int i = 0; i < 10; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }
        System.out.println(data);
    }
}