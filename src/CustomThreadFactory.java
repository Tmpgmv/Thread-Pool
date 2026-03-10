import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


class CustomThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger counter = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this.poolName = poolName;
    }


    @Override
    public Thread newThread(Runnable runnable) {
        String name = poolName + "-worker-" + counter.getAndIncrement();
        System.out.println("Создание нового потока: " + name);
        return new Thread(runnable, name);
    }
}