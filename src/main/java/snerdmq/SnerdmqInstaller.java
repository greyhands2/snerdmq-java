package snerdmq;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

public class SnerdmqInstaller {
    private static final String REPO = "greyhands2/snerdmq";
    private static final String VERSION = "v0.1.1";

    public static String ensureDownloaded() throws IOException, InterruptedException {
        String osName = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        String osArch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);

        String platform;
        if (osName.contains("mac") || osName.contains("darwin")) {
            platform = "macos";
        } else if (osName.contains("win")) {
            platform = "windows";
        } else if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix")) {
            platform = "linux";
        } else {
            throw new RuntimeException("[Snerd] Unsupported OS: " + osName);
        }

        String architecture;
        if (osArch.contains("amd64") || osArch.contains("x86_64")) {
            architecture = "x64";
        } else if (osArch.contains("aarch64") || osArch.contains("arm64")) {
            architecture = "arm64";
        } else {
            throw new RuntimeException("[Snerd] Unsupported Architecture: " + osArch);
        }

        String ext = platform.equals("windows") ? ".exe" : "";
        String binaryName = "snerdmq-" + platform + "-" + architecture + ext;
        String downloadUrl = "https://github.com/" + REPO + "/releases/download/" + VERSION + "/" + binaryName;

        // Place the binary in the user's home directory so it's shared across all Java apps
        String homeDir = System.getProperty("user.home");
        File snerdDir = new File(homeDir, ".snerdmq");
        if (!snerdDir.exists()) {
            snerdDir.mkdirs();
        }

        Path destPath = new File(snerdDir, "snerdmq" + ext).toPath();
        
        if (Files.exists(destPath)) {
            // Already downloaded
            return destPath.toString();
        }

        System.out.println("[Snerd] Downloading pre-compiled engine from GitHub: " + binaryName + "...");

        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(downloadUrl))
                .header("User-Agent", "SnerdMQ-Java-Installer")
                .GET()
                .build();

        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            System.err.println("\n[Snerd] WARN: Binary not found at " + downloadUrl);
            System.err.println("[Snerd] (This is expected if you haven't published a GitHub Release yet)");
            System.err.println("[Snerd] Please provide binary path manually when initializing SnerdQueue.");
            return null; // Don't throw, let the user supply the path manually in tests
        }

        Files.copy(response.body(), destPath, StandardCopyOption.REPLACE_EXISTING);

        if (!platform.equals("windows")) {
            destPath.toFile().setExecutable(true);
        }

        System.out.println("[Snerd] Successfully installed Snerd Engine to " + destPath.toString() + "!");
        return destPath.toString();
    }
    
    // Allows execution via `java -cp snerdmq.jar snerdmq.SnerdmqInstaller`
    public static void main(String[] args) throws Exception {
        ensureDownloaded();
    }
}
