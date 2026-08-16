<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="SnerdMQ Java Logo" />
  <h1>☕ SnerdMQ Java & Kotlin SDK v1.0.1</h1>
  <p>A zero-config, C-speed background job queue for the JVM. Ditch Redis and heavy queue workers for a simple, embedded Rust daemon.</p>
</div>

This is the official JVM SDK wrapper for **SnerdMQ**. It handles all JSON-RPC communication and `ProcessBuilder` orchestration so you can write lightning-fast background jobs in Java, Kotlin, or Scala without managing any external databases like Redis or ActiveMQ.

## ✨ v1.0.1 AI Features
- **Smart API Rate-Limiting**: Natively tracks `rateLimitGroup` execution velocity to prevent 429 "Too Many Requests" API errors.
- **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
- **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
- **Ditch Redis**: Gives your Spring Boot or Ktor apps persistent state, automatic retries, and dead-letter queues right out of the box with zero external infrastructure.
- **Zero Rust Required**: Our built-in `SnerdmqInstaller` class automatically downloads the pre-compiled C-speed Rust binary for your OS.
- **Thread-Safe**: Built on top of native Java `ExecutorService` and `ProcessBuilder`, it is heavily optimized for massively concurrent enterprise workloads.

### ⚙️ Advanced Task Configuration (v1.0.1)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`autoDedupe` (`Boolean`)**: If set to `true`, the daemon computes a cryptographic hash of the `taskType` and `data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgencyScore` (`Double`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rateLimitGroup` (`String`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`maxPerMinute` (`Integer`)**: Used in conjunction with `rateLimitGroup`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`executeAt` (`String` | `DateTime`)**: A timestamp of when the job should be executed in the future.
* **`cron` (`String`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the new scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retryAfterHours` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retryAfterHours` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

## 📦 Installation

This package is designed to work flawlessly in both modern Gradle projects and legacy Maven projects!

### Option A: Gradle (Modern)
Add the dependency to your `build.gradle`:
```groovy
dependencies {
    implementation 'io.github.greyhands2:snerdmq:1.0.1'
}
```

### Option B: Maven (Enterprise)
Add the dependency to your `pom.xml`:
```xml
<dependency>
    <groupId>io.github.greyhands2</groupId>
    <artifactId>snerdmq</artifactId>
    <version>1.0.1</version>
</dependency>
```

---

## ⚡ Quickstart

Using the SDK is incredibly simple. Initialize the queue, register your Consumer callbacks, and start listening!

```java
import snerdmq.SnerdQueue;
import snerdmq.SnerdmqInstaller;

public class App {
    public static void main(String[] args) throws Exception {
        // 1. (Optional) Download the Rust daemon to the user's ~/.snerdmq folder
        SnerdmqInstaller.ensureDownloaded();

        // 2. Initialize the daemon in the background
        SnerdQueue queue = new SnerdQueue();

        // 3. Register your background job logic
        queue.registerHandler("send_email", (jsonData) -> {
            System.out.println("Executing background job with payload: " + jsonData);
            // Throw a RuntimeException here to automatically trigger SnerdMQ's retry logic!
        });

        // 4. Start the async Listener threads
        queue.startListening();
        System.out.println("SnerdMQ Java SDK is listening for jobs...");

        // 5. Enqueue a job from anywhere in your codebase (Now with v1.0.1 AI Features!)
        queue.enqueue(
            "email-123",
            "send_email",
            "{\"to\":\"james.gosling@java.com\",\"subject\":\"SnerdMQ Update\"}",
            3,              // max retries
            0.0,            // retry after hours
            "email_api",    // rateLimitGroup
            100,            // maxPerMinute
            true,           // autoDedupe
            0.99,           // urgencyScore
            null,           // executeAt
            "1h"            // cron: Runs every 1 hour!
        );
        
        // Let the application run
        Thread.sleep(Long.MAX_VALUE);
    }
}
```

### ☠️ Dead Letter Queue (Handling Permanent Failures)

When a task fails repeatedly and exhausts its `maxRetries`, the SnerdMQ daemon permanently moves it to the Dead Letter Queue. You can hook into this event to alert your team, update your database, or send a Slack message by registering a Max Retry Handler.

```java
// 5. Catch tasks that have permanently failed (Dead Letter Queue)
queue.registerMaxRetryHandler("send_email", data -> {
    System.out.println("Email task failed after all retries! Data: " + data);
});
```

---

## 🌍 Advanced: Distributed Scaling

By default, the SDK spins up the Rust daemon which writes the queue to a local file (`.snerdata/tasks/tasks.log`). 

If you have multiple Java microservices running behind a load balancer and want them to share the exact same queue, simply mount a **Shared Network Drive** (like AWS EFS or NFS) to all of your servers and pass the shared path:

```java
// All of your JVM servers point to the exact same shared file!
// SnerdMQ's native OS file-locking guarantees zero data corruption.
SnerdQueue queue = new SnerdQueue(null, "/mnt/aws-efs-shared-drive/snerd_tasks.log");
```

*Built with ❤️ for John Wick tier engineering.*
