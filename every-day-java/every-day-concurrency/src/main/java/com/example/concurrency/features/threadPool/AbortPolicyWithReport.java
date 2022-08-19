package com.example.concurrency.features.threadPool;

import com.example.concurrency.util.ThreadDumpHelper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 描述:
 * AbortPolicy线程拒绝策略
 * 如果线程池已满，退出申请并打印Thread Dump(会有一定的最少间隔，默认为10分钟）
 * @author zed
 */
@Slf4j
public class AbortPolicyWithReport extends ThreadPoolExecutor.AbortPolicy {

    private static final Logger log = LoggerFactory.getLogger(ThreadDumpHelper.class);
    private final String threadName;

    private ThreadDumpHelper dumpHelper = new ThreadDumpHelper();

    public AbortPolicyWithReport(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        String msg = String.format(
                "Thread pool is EXHAUSTED!"
                        + " Thread Name: %s, Pool Size: %d (active: %d, core: %d, max: %d, largest: %d), Task: %d (completed: %d),"
                        + " Executor status:(isShutdown:%s, isTerminated:%s, isTerminating:%s)!",
                threadName, e.getPoolSize(), e.getActiveCount(), e.getCorePoolSize(), e.getMaximumPoolSize(),
                e.getLargestPoolSize(), e.getTaskCount(), e.getCompletedTaskCount(), e.isShutdown(), e.isTerminated(),
                e.isTerminating());
        log.warn(msg);
        dumpHelper.tryThreadDump(null);
        throw new RejectedExecutionException(msg);
    }

}

