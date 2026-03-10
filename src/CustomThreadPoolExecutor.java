import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Исправленная реализация пула потоков
 */
class CustomThreadPoolExecutor implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTimeNanos;
    private final int queueSize;
    private final int minSpareThreads;

    private final List<BlockingQueue<Runnable>> taskQueues;
    private final List<Worker> workers = new CopyOnWriteArrayList<>();
    private final List<Thread> activeThreads = new CopyOnWriteArrayList<>();
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

        this.threadFactory = new CustomThreadFactory("MyPool-worker");

        this.taskQueues = new ArrayList<>();
        for (int i = 0; i < maxPoolSize; i++) {
            taskQueues.add(new LinkedBlockingQueue<>(queueSize));
        }

        // Запускаем core потоки
        for (int i = 0; i < corePoolSize; i++) {
            addWorker(i);
        }
    }

    private void addWorker(int queueIndex) {
        if (threadCount.get() < maxPoolSize && !isShutdown.get()) {
            int targetQueue = queueIndex % maxPoolSize;
            Worker worker = new Worker(taskQueues.get(targetQueue));
            workers.add(worker);

            Thread t = threadFactory.newThread(worker);
            activeThreads.add(t); // Сохраняем Thread
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

        // 1. Создаём новые потоки при нехватке запасных
        long idleThreads = workers.stream().filter(w -> !w.isWorking()).count();
        if (idleThreads < minSpareThreads && threadCount.get() < maxPoolSize) {
            addWorker(threadCount.get());
        }

        // 2. Least Loaded балансировка
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
        private volatile Thread myThread;

        public Worker(BlockingQueue<Runnable> queue) {
            this.myQueue = queue;
        }

        public boolean isWorking() {
            return working;
        }

        public Thread getThread() {
            return myThread;
        }

        @Override
        public void run() {
            this.myThread = Thread.currentThread();
            String threadName = myThread.getName();

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

                    // Проверка shutdown перед выполнением задачи
                    if (isShutdown.get()) {
                        myQueue.clear(); // Очищаем очередь
                        return;
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
                System.out.println("[Worker] " + threadName + " interrupted");
                Thread.currentThread().interrupt(); // Восстанавливаем флаг
            } finally {
                workers.remove(this);
                threadCount.decrementAndGet();
                activeThreads.remove(myThread); // Удаляем из списка
            }
        }
    }

    @Override
    public void shutdown() {
        System.out.println("[Pool] Initiating graceful shutdown...");
        isShutdown.set(true);
        // Потоки завершатся после выполнения текущих задач
    }

    @Override
    public void shutdownNow() {
        System.out.println("[Pool] Initiating immediate shutdown...");
        isShutdown.set(true);

        // Прерываем ВСЕ активные потоки
        activeThreads.forEach(thread -> {
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
            }
        });

        // Очищаем очереди
        taskQueues.forEach(BlockingQueue::clear);
    }
}
