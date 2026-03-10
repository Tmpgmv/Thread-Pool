import java.util.concurrent.TimeUnit;


public class Main {
    public static void main(String[] args) throws InterruptedException {
        CustomThreadPoolExecutor pool = new CustomThreadPoolExecutor(
                2,  // core
                4,  // max
                5, TimeUnit.SECONDS, // keepAlive
                3,  // queueSize per worker
                1   // minSpareThreads
        );

        // Имитация нагрузки
        for (int i = 0; i < 15; i++) {
            final int id = i;
            pool.execute(() -> {
                try {
                    Thread.sleep(1000);
                    System.out.println("   -> Задача " + id + " завершена в " + Thread.currentThread().getName());
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
        }

        Thread.sleep(10000);
        System.out.println("Завершение...");
        pool.shutdown();
    }
}