package com.clevercloud.testcontainers.ceph;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class CephContainerTest {
    private static final String CEPH_IMAGE = "v6.0.3-stable-6.0-pacific-centos-8";
    private static final String RGW_BUCKET_TEST = "testcontainers-ceph";
    private static final String RGW_BUCKET_OBJECT_TEST = "testcontainers-ceph-object";

    @Test
    public void cephDefaultTest() {
        try (CephContainer container = new CephContainer(CEPH_IMAGE)) {
            container.start();
        }
    }

    @Test
    public void cephCustomDaemonsTest() {
        HashSet<String> daemons = new HashSet<>(Arrays.asList("osd", "rgw"));
        try (CephContainer container = new CephContainer(CEPH_IMAGE, daemons)) {
            container.start();
        }
    }

    @Test
    public void cephRGWTest() throws URISyntaxException, IOException {
        HashSet<String> daemons = new HashSet<>(Arrays.asList("osd", "rgw"));
        try (CephContainer container = new CephContainer(CEPH_IMAGE, daemons)) {
            container.start();

            URI endpointUri = container.getRGWUri();
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(container.getRGWAccessKey(), container.getRGWSecretKey());
            S3Client s3 = S3Client.builder().endpointOverride(endpointUri).region(Region.of("default")).credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();
            s3.createBucket(CreateBucketRequest.builder().bucket(RGW_BUCKET_TEST).build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(RGW_BUCKET_TEST).build());

            String body = RandomStringUtils.random(1024);

            s3.putObject(PutObjectRequest.builder().bucket(RGW_BUCKET_TEST).key(RGW_BUCKET_OBJECT_TEST).build(), RequestBody.fromBytes(body.getBytes(StandardCharsets.UTF_8)));
            String responseBody = new String(s3.getObject(GetObjectRequest.builder().bucket(RGW_BUCKET_TEST).key(RGW_BUCKET_OBJECT_TEST).build()).readAllBytes());

            assertEquals(body, responseBody);
        }
    }
}
