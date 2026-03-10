import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Интерфейс управления пулом.
 * Предоставлен в задании.
 * */
interface CustomExecutor extends Executor {
    void execute(Runnable command);
    <T> Future<T> submit(Callable<T> callable);
    void shutdown();
    void shutdownNow();
}