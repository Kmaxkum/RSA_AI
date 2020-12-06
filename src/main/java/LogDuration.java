import java.util.concurrent.TimeUnit;

public class LogDuration {
    private String str;

    private long start;
    private long end;

    LogDuration(String str) {
        this.str = str;
    }

    void start(){
        start = System.nanoTime();
    }

    void end() {
        end = System.nanoTime();
        System.out.println(String.format(
                "Message: %s \nTime: %d ms"
                , str
                , TimeUnit.NANOSECONDS.toMillis(end - start)));
    }
}
