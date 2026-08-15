package snerdmq;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import io.javalin.Javalin;
import io.javalin.websocket.WsContext;


public class SnerdQueue {
    private String binaryPath;
    private String storagePath;
    private final Map<String, Consumer<String>> handlers = new ConcurrentHashMap<>();
    private final Map<String, Consumer<String>> maxRetryHandlers = new ConcurrentHashMap<>();
    private static final ThreadLocal<String> currentTaskId = new ThreadLocal<>();
    private final Set<WsContext> wsClients = ConcurrentHashMap.newKeySet();

    
    private Process process;
    private BufferedWriter writer;
    private BufferedReader reader;
    
    private ExecutorService stdoutReaderPool;
    private ExecutorService jobExecutionPool;
    private volatile boolean isShuttingDown = false;
    private final Map<String, CompletableFuture<Void>> pendingEnqueues = new ConcurrentHashMap<>();

    public SnerdQueue() throws IOException, InterruptedException {
        this(null, null);
    }

    public SnerdQueue(String binaryPath) throws IOException, InterruptedException {
        this(binaryPath, null);
    }

    public SnerdQueue(String binaryPath, String storagePath) throws IOException, InterruptedException {
        this.binaryPath = binaryPath;
        this.storagePath = storagePath;

        if (this.binaryPath == null) {
            this.binaryPath = SnerdmqInstaller.ensureDownloaded();
        }

        if (this.binaryPath == null) {
            throw new RuntimeException("[Snerd] Binary path cannot be null. Installer failed or path not provided.");
        }
    }

    public void registerHandler(String taskType, Consumer<String> callback) {
        handlers.put(taskType, callback);
        if (process != null && process.isAlive() && !isShuttingDown) {
            sendMessage(String.format("{\"action\":\"register\",\"task_type\":\"%s\"}", taskType));
        }
    }

    public void registerMaxRetryHandler(String taskType, Consumer<String> callback) {
        maxRetryHandlers.put(taskType, callback);
    }

    public void startListening() throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add(this.binaryPath);
        if (this.storagePath != null) {
            cmd.add(this.storagePath);
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        this.process = pb.start();

        this.writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        this.reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        
        // Background thread to constantly read standard output from Rust
        this.stdoutReaderPool = Executors.newSingleThreadExecutor();
        // Background thread pool to execute the user's jobs concurrently
        this.jobExecutionPool = Executors.newCachedThreadPool();

        this.stdoutReaderPool.submit(() -> {
            try {
                String line;
                while (!isShuttingDown && (line = reader.readLine()) != null) {
                    handleLine(line.trim());
                }
            } catch (IOException e) {
                if (!isShuttingDown) e.printStackTrace();
            }
        });

        // Re-register existing handlers with the newly spawned daemon
        for (String taskType : handlers.keySet()) {
            sendMessage(String.format("{\"action\":\"register\",\"task_type\":\"%s\"}", taskType));
        }
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, null, null, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute, Boolean autoDedupe) {
        return enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, autoDedupe, null);
    }

    public CompletableFuture<Void> enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours, String rateLimitGroup, Integer maxPerMinute, Boolean autoDedupe, Double urgencyScore) {
        if (process == null || !process.isAlive() || isShuttingDown) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("[Snerd] Cannot enqueue task: Queue is not running."));
            return future;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingEnqueues.put(taskId, future);

        // We escape the inner JSON string safely
        String escapedJson = jsonData.replace("\"", "\\\"");
        
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append(String.format(
            Locale.US,
            "{\"action\":\"enqueue\",\"task_id\":\"%s\",\"task_type\":\"%s\",\"task_data\":\"%s\",\"max_retries\":%d,\"retry_after_hours\":%.2f",
            taskId, taskType, escapedJson, maxRetries, retryAfterHours
        ));
        
        if (rateLimitGroup != null) {
            jsonBuilder.append(String.format(",\"rate_limit_group\":\"%s\"", rateLimitGroup));
        }
        if (maxPerMinute != null) {
            jsonBuilder.append(String.format(",\"max_per_minute\":%d", maxPerMinute));
        }
        
        if (autoDedupe != null) { jsonBuilder.append(String.format(",\"auto_dedupe\":%b", autoDedupe)); }
        if (urgencyScore != null) { jsonBuilder.append(String.format(java.util.Locale.US, ",\"urgency_score\":%.2f", urgencyScore)); }
        jsonBuilder.append("}");
        
        sendMessage(jsonBuilder.toString());
        return future;
    }

    public void shutdown() {
        this.isShuttingDown = true;
        
        if (stdoutReaderPool != null) stdoutReaderPool.shutdownNow();
        if (jobExecutionPool != null) jobExecutionPool.shutdown();

        if (process != null && process.isAlive()) {
            process.destroy();
        }
    }

    private synchronized void sendMessage(String json) {
        if (isShuttingDown || writer == null) return;
        try {
            writer.write(json);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleLine(String line) {
        if (line.isEmpty()) return;

        // Note: For a zero-dependency Java library, we use simple RegEx to parse the JSON-RPC.
        // In a massive framework, users would provide Jackson, but we don't want to force Jackson in the SDK jar.
        
        String action = extractJsonField(line, "action");
        if (action == null) return;

        if (action.equals("execute")) {
            String taskId = extractJsonField(line, "task_id");
            String taskType = extractJsonField(line, "task_type");
            String taskData = extractJsonField(line, "task_data");

            if (taskId == null || taskType == null) return;

            Consumer<String> handler = handlers.get(taskType);
            
            if (handler == null) {
                sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"error\",\"error_msg\":\"No handler registered\"}", taskId));
                return;
            }

            // Execute on the cached thread pool so we don't block the stdout reader!
            jobExecutionPool.submit(() -> {
                try {
                    currentTaskId.set(taskId);
                    String unescapedData = taskData != null ? taskData.replace("\\\"", "\"").replace("\\\\", "\\") : "";
                    handler.accept(unescapedData);
                    sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"success\"}", taskId));
                } catch (Exception e) {
                    String errorMsg = e.getMessage() != null ? e.getMessage().replace("\"", "'") : "Unknown Exception";
                    sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"error\",\"error_msg\":\"%s\"}", taskId, errorMsg));
                }
            });
            
        } else if (action.equals("ack")) {
            String taskId = extractJsonField(line, "task_id");
            if (taskId != null) {
                CompletableFuture<Void> future = pendingEnqueues.remove(taskId);
                if (future != null) future.complete(null);
            }
        } else if (action.equals("error")) {
            String taskId = extractJsonField(line, "task_id");
            String message = extractJsonField(line, "message");
            if (taskId != null) {
                CompletableFuture<Void> future = pendingEnqueues.remove(taskId);
                if (future != null) future.completeExceptionally(new RuntimeException(message));
            } else {
                System.err.println("[Snerd] Error from engine: " + message);
            }
        } else if (action.equals("progress")) {
            for (WsContext ctx : wsClients) {
                if (ctx.session.isOpen()) {
                    ctx.send(line); // Forward the raw JSON string
                }
            }
        } else if (action.equals("max_retries_reached")) {
            String taskId = extractJsonField(line, "task_id");
            String taskType = extractJsonField(line, "task_type");
            
            Consumer<String> handler = maxRetryHandlers.get(taskType);
            if (handler != null && taskId != null) {
                String taskData = extractJsonField(line, "task_data");
                jobExecutionPool.submit(() -> {
                    try {
                        currentTaskId.set(taskId);
                        String unescapedData = taskData != null ? taskData.replace("\\\"", "\"").replace("\\\\", "\\") : "";
                        handler.accept(unescapedData);
                    } catch (Exception e) {
                        System.err.println("[Snerd] Error in max retry handler for task " + taskId + ": " + e.getMessage());
                    }
                });
            } else {
                System.out.println("[Snerd] Dead Letter Queue: Task " + taskId + " (" + taskType + ") permanently failed.");
            }
        }
    }

    // A lightweight helper to extract flat string fields from a JSON payload
    private String extractJsonField(String json, String key) {
        Pattern pattern = Pattern.compile("\"" + key + "\"\\s*:\\s*\"(.*?)(?<!\\\\)\"");
        Matcher matcher = pattern.matcher(json);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    public void yieldProgress(String data) {
        String taskId = currentTaskId.get();
        if (taskId == null) {
            throw new RuntimeException("[Snerd] yieldProgress must be called within a task handler context.");
        }
        
        String escapedData = data != null ? data.replace("\"", "\\\"") : "";
        String msg = String.format("{\"action\":\"progress\",\"task_id\":\"%s\",\"data\":\"%s\"}", taskId, escapedData);
        sendMessage(msg);
    }

    public void startDashboard(int port) {
        Javalin app = Javalin.create(config -> {
            config.plugins.enableCors(cors -> {
                cors.add(it -> it.anyHost());
            });
        }).start(port);

        app.get("/", ctx -> {
            Path htmlPath = Paths.get(System.getProperty("user.dir"), "static", "index.html");
            if (Files.exists(htmlPath)) {
                ctx.contentType("text/html");
                ctx.result(Files.readString(htmlPath));
            } else {
                ctx.status(404).result("Dashboard UI not found in static folder.");
            }
        });

        app.get("/api/stats", ctx -> {
            int enqueued = 0, processed = 0, failed = 0;
            Path tasksPath = Paths.get(this.storagePath != null ? this.storagePath : "./.snerdata", "tasks", "tasks.log");
            if (Files.exists(tasksPath)) {
                try {
                    for (String line : Files.readAllLines(tasksPath)) {
                        if (line.trim().isEmpty()) continue;
                        enqueued++;
                        if (line.contains("\"deletedAt\":\"")) {
                            if (line.contains("\"lastJobError\":\"")) {
                                failed++;
                            } else {
                                processed++;
                            }
                        }
                    }
                } catch (Exception e) {}
            }
            String result = String.format("{\"enqueued\":%d,\"processed\":%d,\"failed\":%d}", enqueued, processed, failed);
            ctx.contentType("application/json").result(result);
        });

        app.get("/api/tasks", ctx -> {
            Map<String, String> tasksMap = new java.util.LinkedHashMap<>();
            Path tasksPath = Paths.get(this.storagePath != null ? this.storagePath : "./.snerdata", "tasks", "tasks.log");
            if (Files.exists(tasksPath)) {
                try {
                    for (String line : Files.readAllLines(tasksPath)) {
                        if (line.trim().isEmpty()) continue;
                        String tId = extractJsonField(line, "taskId");
                        if (tId != null) tasksMap.put(tId, line);
                    }
                } catch (Exception e) {}
            }

            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (String t : tasksMap.values()) {
                String tId = extractJsonField(t, "taskId");
                String tType = extractJsonField(t, "taskType");
                String status;
                if (t.contains("\"deletedAt\":\"")) {
                    status = t.contains("\"lastJobError\":\"") ? "failed" : "completed";
                } else {
                    status = t.contains("\"lastJobError\":\"") ? "failed" : "queued";
                }
                
                String rCount = extractJsonField(t, "retryCount");
                String mRetries = extractJsonField(t, "maxRetries");
                String rAfter = extractJsonField(t, "retryAfterTime");
                
                if (!first) sb.append(",");
                sb.append(String.format("{\"id\":\"%s\",\"type\":\"%s\",\"status\":\"%s\",\"progress\":0", tId, tType, status));
                if (rCount != null) sb.append(",\"retryCount\":").append(rCount);
                if (mRetries != null) sb.append(",\"maxRetries\":").append(mRetries);
                if (rAfter != null) sb.append(",\"retryAfterTime\":\"").append(rAfter).append("\"");
                sb.append("}");
                first = false;
            }
            sb.append("]");
            ctx.contentType("application/json").result(sb.toString());
        });

        app.ws("/ws", ws -> {
            ws.onConnect(ctx -> wsClients.add(ctx));
            ws.onClose(ctx -> wsClients.remove(ctx));
            ws.onError(ctx -> wsClients.remove(ctx));
        });

        System.out.println("[Snerd] Dashboard running on http://localhost:" + port);
    }

}
