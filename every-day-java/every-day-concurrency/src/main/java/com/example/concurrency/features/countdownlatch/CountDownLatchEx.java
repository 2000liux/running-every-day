package com.example.concurrency.features.countdownlatch;

import com.example.concurrency.features.threadPool.ThreadPoolBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 描述:
 * CountDownLatchEx 中文意思 倒计时 主要用来解决一个线程等待多个线程的场景
 *
 * @author zed
 * @since 2019-06-18 10:20 AM
 */
public class CountDownLatchEx {
    public static void main(String[] args) throws InterruptedException{
        // 创建 2 个线程的线程池
        ThreadPoolExecutor executor = ThreadPoolBuilder.fixedPool().setPoolSize(2).build();
        // 计数器初始化为 2
        CountDownLatch latch = new CountDownLatch(2);
        executor.execute(()-> {
            System.out.println("T1, " + Thread.currentThread().getName());
            latch.countDown();
        });
        executor.execute(()-> {
            System.out.println("T2, " + Thread.currentThread().getName());
            latch.countDown();
        });
        // 等待两个查询操作结束
        latch.await(1, TimeUnit.SECONDS);
        System.out.println("T3, " + Thread.currentThread().getName());
        executor.shutdown();
    }
}

