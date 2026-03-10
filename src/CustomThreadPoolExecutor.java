import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPoolExecutor implements CustomExecutor {
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
        this.threadFactory = new CustomThreadFactory("CustomPool");

        // Инициализируем очереди (по одной на потенциальный воркер для изоляции)
        this.taskQueues = new ArrayList<>();
        for (int i = 0; i < maxPoolSize; i++) {
            taskQueues.add(new LinkedBlockingQueue<>(queueSize));
        }

        // Запуск базовых потоков
        for (int i = 0; i < corePoolSize; i++) {
            addWorker(i);
        }
    }

    private void addWorker(int queueIndex) {
        if (threadCount.get() < maxPoolSize) {
            Worker worker = new Worker(taskQueues.get(queueIndex % maxPoolSize));
            workers.add(worker);
            threadFactory.newThread(worker).start();
            threadCount.incrementAndGet();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown.get()) throw new RejectedExecutionException("Пул закрыт.");

        // 1. Проверка minSpareThreads (упреждающее создание)
        long busyThreads = workers.stream().filter(Worker::isWorking).count();
        if (threadCount.get() - busyThreads < minSpareThreads && threadCount.get() < maxPoolSize) {
            addWorker(threadCount.get());
        }

        // 2. Балансировка: Least Loaded
        BlockingQueue<Runnable> targetQueue = taskQueues.stream()
                .min(Comparator.comparingInt(Collection::size))
                .orElse(taskQueues.get(0));

        if (targetQueue.offer(command)) {
            System.out.println("Задача помещена в очередь. Размер: " + targetQueue.size());
        } else {
            handleReject(command);
        }
    }

    private void handleReject(Runnable command) {
        // Выбранная политика: Abort + Log.
        // Лучше сразу сообщить клиенту о перегрузке,
        // чем бесконечно копить задачи и раздувать задержки.
        System.err.println("Задача отклонена из-за перегруженности системы.");
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<>(callable);
        execute(task);
        return task;
    }

    // Внутренний класс Worker
    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> myQueue;
        private volatile boolean working = false;

        public Worker(BlockingQueue<Runnable> queue) { this.myQueue = queue; }
        public boolean isWorking() { return working; }

        @Override
        public void run() {
            try {
                while (!isShutdown.get() || !myQueue.isEmpty()) {
                    Runnable task = myQueue.poll(keepAliveTimeNanos, TimeUnit.NANOSECONDS);

                    if (task == null) {
                        if (threadCount.get() > corePoolSize) {
                            System.out.println(Thread.currentThread().getName() + " время истекло, " +
                                    "останавливаем.");
                            break;
                        }
                        continue;
                    }

                    working = true;
                    System.out.println(Thread.currentThread().getName() + " выполняет задачу.");
                    task.run();
                    working = false;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                workers.remove(this);
                threadCount.decrementAndGet();
                System.out.println(Thread.currentThread().getName() + " остановлен.");
            }
        }
    }

    @Override public void shutdown() { isShutdown.set(true); }
    @Override public void shutdownNow() { isShutdown.set(true); workers.clear(); }
}