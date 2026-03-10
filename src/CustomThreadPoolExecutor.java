import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Реализация пула потоков
 */
class CustomThreadPoolExecutor implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTimeNanos;
    private final int queueSize;
    private final int minSpareThreads;

    private final List<BlockingQueue<Runnable>> taskQueues;
    private final List<Worker> workers = new CopyOnWriteArrayList<>();
    private final ThreadFactory threadFactory;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicInteger threadCount = new AtomicInteger(0);

    public CustomThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime,
                                    TimeUnit unit, int queueSize, int minSpareThreads) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTimeNanos = unit.toNanos(keepAliveTime);
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;

        // Используем внешнюю фабрику с loggingWrapper
        this.threadFactory = new CustomThreadFactory("MyPool-worker");

        this.taskQueues = new ArrayList<>();
        for (int i = 0; i < maxPoolSize; i++) {
            taskQueues.add(new LinkedBlockingQueue<>(queueSize));
        }

        for (int i = 0; i < corePoolSize; i++) {
            addWorker(i);
        }
    }

    private void addWorker(int queueIndex) {
        if (threadCount.get() < maxPoolSize) {
            Worker worker = new Worker(taskQueues.get(queueIndex % maxPoolSize));
            workers.add(worker);
            Thread t = threadFactory.newThread(worker);
            t.start();
            threadCount.incrementAndGet();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown.get()) {
            handleReject(command);
            return;
        }

        // 1. Упреждающее создание при нехватке "запасных" (minSpareThreads)
        long idleThreads = workers.stream().filter(w -> !w.isWorking()).count();
        if (idleThreads < minSpareThreads && threadCount.get() < maxPoolSize) {
            addWorker(threadCount.get());
        }

        // 2. Балансировка: Least Loaded
        int targetIndex = 0;
        BlockingQueue<Runnable> targetQueue = taskQueues.get(0);
        int minSize = Integer.MAX_VALUE;

        for (int i = 0; i < taskQueues.size(); i++) {
            int currentSize = taskQueues.get(i).size();
            if (currentSize < minSize) {
                minSize = currentSize;
                targetQueue = taskQueues.get(i);
                targetIndex = i;
            }
        }

        if (targetQueue.offer(command)) {
            System.out.println("[Pool] Task accepted into queue #" + targetIndex);
        } else {
            handleReject(command);
        }
    }

    private void handleReject(Runnable command) {
        System.err.println("[Rejected] Task " + command + " was rejected due to overload!");
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<>(callable);
        execute(task);
        return task;
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> myQueue;
        private volatile boolean working = false;

        public Worker(BlockingQueue<Runnable> queue) { this.myQueue = queue; }
        public boolean isWorking() { return working; }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            try {
                while (!isShutdown.get() || !myQueue.isEmpty()) {
                    Runnable task = myQueue.poll(keepAliveTimeNanos, TimeUnit.NANOSECONDS);

                    if (task == null) {
                        if (threadCount.get() > corePoolSize) {
                            System.out.println("[Worker] " + threadName + " idle timeout, stopping.");
                            return;
                        }
                        continue;
                    }

                    working = true;
                    System.out.println("[Worker] " + threadName + " executes task");
                    try {
                        task.run();
                    } catch (Exception e) {
                        System.err.println("[Worker] Task error: " + e.getMessage());
                    } finally {
                        working = false;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                workers.remove(this);
                threadCount.decrementAndGet();
                // Лог "terminated" выведет ThreadFactory через wrapper
            }
        }
    }

    @Override public void shutdown() { isShutdown.set(true); }
    @Override public void shutdownNow() { isShutdown.set(true); workers.clear(); }
}