# testcontainers-ceph

This implements a Java [testcontainer](https://www.testcontainers.org/) for
[Ceph](https://ceph.io/), a distributed object, block, and file storage platform.

## Usage example

```java
import com.clevercloud.testcontainers.ceph.CephContainer;

try (CephContainer container = new CephContainer(CEPH_VERSION)) {
  // Start the container. This step might take some time...
  container.start();

  URI endpointUri = container.getRGWUri();
  AwsBasicCredentials awsCreds = AwsBasicCredentials.create(container.getRGWAccessKey(), container.getRGWSecretKey());
  S3Client s3 = S3Client.builder().endpointOverride(endpointUri).region(Region.of("default")).credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();
  s3.createBucket(CreateBucketRequest.builder().bucket("a-bucket-to-create").build());
  s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket("a-bucket-to-create").build());

  String body = RandomStringUtils.random(1024);

  s3.putObject(PutObjectRequest.builder().bucket("a-bucket-to-create").key("an-object-to-create").build(), RequestBody.fromBytes(body.getBytes(StandardCharsets.UTF_8)));
  String responseBody = new String(s3.getObject(GetObjectRequest.builder().bucket("a-bucket-to-create").key("an-object-tocreate").build()).readAllBytes());
}
```

(Disclaimer: this code will not compile. It's just so you get an idea!)

For more examples, see `src/test/java/com/clevercloud/testcontainers/ceph/CephContainerTest.java`

## Upstream status

There is currently [an upstream pull request](https://github.com/testcontainers/testcontainers-java/pull/2687) but this library was done before seeing it. If a ceph module ever gets merged upstream, we'll deprecate this one.

