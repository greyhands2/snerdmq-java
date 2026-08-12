package snerdmq;

import org.junit.jupiter.api.Test;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnerdQueueTest {

    @Test
    public void testEndToEndExecution() throws Exception {
        System.out.println("🚀 Booting up Java SnerdMQ Test App...");

        // Link directly to the local compiled daemon for this test
        String osName = System.getProperty("os.name").toLowerCase();
        String ext = osName.contains("win") ? ".exe" : "";
        File localBinary = new File("../snerdmq/target/debug/snerdmq" + ext);
        
        File dbFile = new File("../.snerdata/tasks/tasks.log");
        if (dbFile.exists()) {
            dbFile.delete(); // Start clean
        }

        SnerdQueue queue = new SnerdQueue(localBinary.getAbsolutePath());

        // CountDownLatch is perfect for pausing the main JUnit thread while the background executor runs the job!
        CountDownLatch latch = new CountDownLatch(1);

        queue.registerHandler("test_java_job", (json) -> {
            System.out.println("\n✅ Java App received job! Data: " + json);
            if (json.contains("James Gosling")) {
                latch.countDown();
            } else {
                throw new RuntimeException("Assertion failed: message did not match");
            }
        });

        queue.startListening();
        
        // Give daemon a tiny fraction of a second to boot up
        Thread.sleep(100);

        System.out.println("Enqueuing job to Rust daemon...");
        queue.enqueue(
            "java-job-1",
            "test_java_job",
            "{\"user_id\":\"java_master\",\"message\":\"James Gosling\"}",
            3,
            0.0
        );

        // Wait up to 5 seconds for the background pool to process the job and hit the latch
        boolean success = latch.await(5, TimeUnit.SECONDS);

        System.out.println("🎉 Job processed successfully. Shutting down.");
        queue.shutdown();

        assertTrue(success, "The background job did not complete in time!");
    }
}
