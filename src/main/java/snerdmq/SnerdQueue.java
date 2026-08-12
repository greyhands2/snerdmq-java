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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SnerdQueue {
    private String binaryPath;
    private String storagePath;
    private final Map<String, Consumer<String>> handlers = new ConcurrentHashMap<>();
    
    private Process process;
    private BufferedWriter writer;
    private BufferedReader reader;
    
    private ExecutorService stdoutReaderPool;
    private ExecutorService jobExecutionPool;
    private volatile boolean isShuttingDown = false;

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

    public void enqueue(String taskId, String taskType, String jsonData, int maxRetries, double retryAfterHours) {
        if (process == null || !process.isAlive() || isShuttingDown) {
            throw new RuntimeException("[Snerd] Cannot enqueue task: Queue is not running.");
        }

        // We escape the inner JSON string safely
        String escapedJson = jsonData.replace("\"", "\\\"");
        
        String msg = String.format(
            Locale.US,
            "{\"action\":\"enqueue\",\"task_id\":\"%s\",\"task_type\":\"%s\",\"task_data\":\"%s\",\"max_retries\":%d,\"retry_after_hours\":%.2f}",
            taskId, taskType, escapedJson, maxRetries, retryAfterHours
        );
        sendMessage(msg);
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
                    // taskData is typically returned as an escaped string from our simple regex
                    String unescapedData = taskData != null ? taskData.replace("\\\"", "\"").replace("\\\\", "\\") : "";
                    handler.accept(unescapedData);
                    sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"success\"}", taskId));
                } catch (Exception e) {
                    String errorMsg = e.getMessage() != null ? e.getMessage().replace("\"", "'") : "Unknown Exception";
                    sendMessage(String.format("{\"action\":\"result\",\"task_id\":\"%s\",\"status\":\"error\",\"error_msg\":\"%s\"}", taskId, errorMsg));
                }
            });
            
        } else if (action.equals("max_retries_reached")) {
            String taskId = extractJsonField(line, "task_id");
            String taskType = extractJsonField(line, "task_type");
            System.out.println("[Snerd] Dead Letter Queue: Task " + taskId + " (" + taskType + ") permanently failed.");
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
}
