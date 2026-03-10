import java.util.concurrent.TimeUnit;


/**
 * Тестовый запуск
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        CustomThreadPoolExecutor pool = new CustomThreadPoolExecutor(
                2, 4, 5, 5
        );

        // Посылаем 50 задач, чтобы вызвать переполнение.
        for (int i = 0; i < 50; i++) {
            final int id = i;
            pool.execute(() -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
        }

        Thread.sleep(6000); // Ждем завершения и idle timeout.
        System.out.println("Main: Calling shutdown...");
        pool.shutdown();
    }
}