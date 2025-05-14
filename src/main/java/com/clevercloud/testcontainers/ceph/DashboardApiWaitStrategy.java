package com.clevercloud.testcontainers.ceph;

import org.testcontainers.containers.wait.strategy.WaitStrategy;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DashboardApiWaitStrategy implements WaitStrategy {
    private int port;
    private String username;
    private String password;
    private Duration timeout = Duration.ofMinutes(5);

    public DashboardApiWaitStrategy withPort(int port) {
        this.port = port;
        return this;
    }

    public DashboardApiWaitStrategy withUsername(String username) {
        this.username = username;
        return this;
    }

    public DashboardApiWaitStrategy withPassword(String password) {
        this.password = password;
        return this;
    }

    public DashboardApiWaitStrategy withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public WaitStrategy withStartupTimeout(Duration startupTimeout) {
        this.timeout = startupTimeout;
        return this;
    }

    private static final Logger log = LoggerFactory.getLogger(DashboardApiWaitStrategy.class);

    @Override
    public void waitUntilReady(org.testcontainers.containers.wait.strategy.WaitStrategyTarget container) {
        long start = System.currentTimeMillis();
        Exception lastException = null;
        String address = container.getHost();
        Integer mappedPort = container.getMappedPort(port);
        String urlStr = String.format("http://%s:%d/api/auth", address, mappedPort);
        String body = String.format("{\"username\": \"%s\", \"password\": \"%s\"}", username, password);
        byte[] postData = body.getBytes(StandardCharsets.UTF_8);
        int tryCount = 0;
        while (System.currentTimeMillis() - start < timeout.toMillis()) {
            tryCount++;
            try {
                URL url = new URL(urlStr);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("accept", "application/vnd.ceph.api.v1.0+json");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(postData);
                }
                int responseCode = conn.getResponseCode();
                log.info("[DashboardApiWaitStrategy] Try #{}: POST {} with body {} -> HTTP {}", tryCount, urlStr, body,
                        responseCode);
                if (responseCode == 201) {
                    log.info("[DashboardApiWaitStrategy] Dashboard API ready after {} tries.", tryCount);
                    return;
                }
            } catch (Exception e) {
                log.warn("[DashboardApiWaitStrategy] Try #{}: Exception during POST {} with body {}: {}", tryCount,
                        urlStr, body, e.getMessage());
                lastException = e;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        log.error("[DashboardApiWaitStrategy] Dashboard API did not become ready after {} tries.", tryCount);
        throw new IllegalStateException("Dashboard API did not become ready in time", lastException);
    }
}
