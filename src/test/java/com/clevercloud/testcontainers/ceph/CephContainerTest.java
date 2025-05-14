package com.clevercloud.testcontainers.ceph;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class CephContainerTest {
    private static final Logger log = LoggerFactory.getLogger(CephContainerTest.class);

    private static final String CEPH_IMAGE = "reef-20250513";
    private static final String RGW_BUCKET_TEST = "testcontainers-ceph";
    private static final String RGW_BUCKET_OBJECT_TEST = "testcontainers-ceph-object";
    private static final String MGR_USERNAME = "admin";
    private static final String MGR_PASSWORD = "admin";
    private static final Integer MGR_PORT = 8080;

    @Test
    public void cephDefaultTest() {
        try (CephContainer container = new CephContainer(CEPH_IMAGE)) {
            container.start();
            container.followOutput(new Slf4jLogConsumer(log).withPrefix("CEPH"));
            container.stop();
        }
    }

    @Test
    public void cephRBDTest() {
        HashSet<String> features = new HashSet<>(Arrays.asList("rbd"));
        try (CephContainer container = new CephContainer(CEPH_IMAGE, features)) {
            container.start();

            container.followOutput(new Slf4jLogConsumer(log).withPrefix("CEPH"));
            String address = container.getHost();
            Integer mappedPort = container.getMappedPort(MGR_PORT);
            String urlStr = String.format("http://%s:%d/api/auth", address, mappedPort);
            String body = String.format("{\"username\": \"%s\", \"password\": \"%s\"}", MGR_USERNAME, MGR_PASSWORD);
            byte[] postData = body.getBytes(StandardCharsets.UTF_8);

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
                assertEquals(201, responseCode);
            } catch (Exception e) {
                log.warn("Exception during POST {} with body {}: {}", urlStr, body, e.getMessage());
                throw new RuntimeException(e);
            }

            container.stop();
        }
    }

    @Test
    public void cephRGWTest() throws URISyntaxException, IOException {
        HashSet<String> features = new HashSet<>(Arrays.asList("rbd", "radosgw"));

        try (CephContainer container = new CephContainer(CEPH_IMAGE, features)) {
            container.start();
            container.followOutput(new Slf4jLogConsumer(log).withPrefix("CEPH"));

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

            container.stop();
        }
    }
}
