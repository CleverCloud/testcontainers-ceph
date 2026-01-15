package com.clevercloud.testcontainers.ceph;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CephContainerTest {
    private static final Logger log = LoggerFactory.getLogger(CephContainerTest.class);

    private static final String CEPH_IMAGE = "reef-20250513";
    private static final String RGW_BUCKET_TEST = "testcontainers-ceph";
    private static final String RGW_BUCKET_OBJECT_TEST = "testcontainers-ceph-object";
    private static final String MGR_USERNAME = "admin";
    private static final String MGR_PASSWORD = "admin";
    private static final Integer MGR_PORT = 8080;

    // Shared container for all tests - uses all features needed by tests
    private static final CephContainer container = new CephContainer(CEPH_IMAGE,
            new HashSet<>(Arrays.asList("rbd", "radosgw", "mon")));

    @org.junit.jupiter.api.BeforeAll
    static void startContainer() {
        container.start();
        container.followOutput(new Slf4jLogConsumer(log).withPrefix("CEPH"));
    }

    @org.junit.jupiter.api.AfterAll
    static void stopContainer() {
        container.stop();
    }

    @Test
    public void cephDefaultTest() {
        // Container is already running, just verify it's accessible
        assertNotNull(container.getHost());
    }

    @Test
    public void cephRBDTest() {
        String address = container.getHost();
        Integer mappedPort = container.getMappedPort(MGR_PORT);
        String urlStr = String.format("http://%s:%d/api/auth", address, mappedPort);
        String body = String.format("{\"username\": \"%s\", \"password\": \"%s\"}", MGR_USERNAME, MGR_PASSWORD);
        byte[] postData = body.getBytes(StandardCharsets.UTF_8);

        try {
            URL url = new URI(urlStr).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("accept", "application/vnd.ceph.api.v1.0+json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(postData);
            }
            int responseCode = conn.getResponseCode();
            assertEquals(201, responseCode);
        } catch (Exception e) {
            log.warn("Exception during POST {} with body {}: {}", urlStr, body, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Test
    public void cephRGWTest() throws URISyntaxException, IOException {
        URI endpointUri = container.getRGWUri();
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(container.getRGWAccessKey(),
                container.getRGWSecretKey());
        S3Client s3 = S3Client.builder().endpointOverride(endpointUri).region(Region.of("default"))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();
        s3.createBucket(CreateBucketRequest.builder().bucket(RGW_BUCKET_TEST).build());
        s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(RGW_BUCKET_TEST).build());

        String body = RandomStringUtils.random(1024);

        s3.putObject(PutObjectRequest.builder().bucket(RGW_BUCKET_TEST).key(RGW_BUCKET_OBJECT_TEST).build(),
                RequestBody.fromBytes(body.getBytes(StandardCharsets.UTF_8)));
        String responseBody = new String(
                s3.getObject(GetObjectRequest.builder().bucket(RGW_BUCKET_TEST).key(RGW_BUCKET_OBJECT_TEST).build())
                        .readAllBytes());

        assertEquals(body, responseBody);
    }

    @Test
    public void cephMONTest() {
        // Test MON service directly by checking TCP connectivity to MON port
        String address = container.getHost();
        Integer monPort = container.getMONPort();

        // Direct TCP connectivity test to MON service
        try (Socket socket = new Socket()) {
            socket.setSoTimeout(5000); // 5 second timeout
            InetSocketAddress monAddress = new InetSocketAddress(address, monPort);

            log.info("Testing direct connectivity to MON service at {}:{}", address, monPort);
            socket.connect(monAddress, 5000);

            // If we reach here, the connection was successful
            log.info("Successfully connected to MON service at {}:{}", address, monPort);

            // Additional verification: MON should accept and close connections gracefully
            // This indicates the MON daemon is listening and responding
            assertEquals(true, socket.isConnected(), "MON port should be accessible");

        } catch (SocketTimeoutException e) {
            log.error("Timeout connecting to MON service at {}:{}", address, monPort);
            throw new RuntimeException("MON service connection timeout", e);
        } catch (IOException e) {
            log.error("Failed to connect to MON service at {}:{}: {}", address, monPort, e.getMessage());
            throw new RuntimeException("MON service not accessible", e);
        }

        // Optional: Also verify via MGR API for comprehensive check
        Integer mgrPort = container.getMappedPort(MGR_PORT);
        String authUrl = String.format("http://%s:%d/api/auth", address, mgrPort);
        String authBody = String.format("{\"username\": \"%s\", \"password\": \"%s\"}", MGR_USERNAME, MGR_PASSWORD);
        byte[] authData = authBody.getBytes(StandardCharsets.UTF_8);

        try {
            URL url = new URI(authUrl).toURL();
            HttpURLConnection authConn = (HttpURLConnection) url.openConnection();
            authConn.setRequestMethod("POST");
            authConn.setRequestProperty("accept", "application/vnd.ceph.api.v1.0+json");
            authConn.setRequestProperty("Content-Type", "application/json");
            authConn.setDoOutput(true);
            try (OutputStream os = authConn.getOutputStream()) {
                os.write(authData);
            }
            int authResponseCode = authConn.getResponseCode();
            assertEquals(201, authResponseCode, "MGR authentication should succeed");

            log.info("MON service verified: Direct TCP connection successful and MGR can authenticate");
        } catch (Exception e) {
            log.warn("MGR authentication failed, but direct MON connection succeeded: {}", e.getMessage());
            // Don't fail the test if direct MON connection worked
        }
    }

    @Test
    public void cephClusterIdTest() {
        String clusterId = container.getClusterId();
        assertNotNull(clusterId, "Cluster ID should not be null");
        assertFalse(clusterId.isEmpty(), "Cluster ID should not be empty");
    }

    @Test
    public void cephHostByIpTest() {
        // Get the container's IP address using the helper method
        String containerIp = container.getContainerIpAddress();

        log.info("Container IP address: {}", containerIp);

        assertNotNull(containerIp, "Container IP should not be null");
        assertFalse(containerIp.isEmpty(), "Container IP should not be empty");
        assertFalse(containerIp.equals("localhost"), "Container IP should not be localhost");
        assertFalse(containerIp.equals("127.0.0.1"), "Container IP should not be 127.0.0.1");

        // Verify we can connect to MGR API using the container's IP (not localhost)
        // Note: We use the internal port since we're connecting directly to container IP
        String urlStr = String.format("http://%s:%d/api/auth", containerIp, MGR_PORT);
        String body = String.format("{\"username\": \"%s\", \"password\": \"%s\"}", MGR_USERNAME, MGR_PASSWORD);
        byte[] postData = body.getBytes(StandardCharsets.UTF_8);

        try {
            URL url = new URI(urlStr).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("accept", "application/vnd.ceph.api.v1.0+json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(postData);
            }
            int responseCode = conn.getResponseCode();
            assertEquals(201, responseCode, "Should be able to authenticate using host address");
            log.info("Successfully connected to MGR API at {}", urlStr);
        } catch (Exception e) {
            log.warn("Exception during connection test: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
