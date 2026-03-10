import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Единая фабрика потоков с логированием создания и завершения
 */
class CustomThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger counter = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String threadName = poolName + "-" + counter.getAndIncrement();

        // Обертка для перехвата завершения потока
        Runnable loggingWrapper = () -> {
            try {
                runnable.run();
            } finally {
                // Это сообщение появится ВСЕГДА при выходе из метода run
                System.out.println("[Worker] " + threadName + " terminated.");
            }
        };

        System.out.println("[ThreadFactory] Creating new thread: " + threadName);
        return new Thread(loggingWrapper, threadName);
    }
}