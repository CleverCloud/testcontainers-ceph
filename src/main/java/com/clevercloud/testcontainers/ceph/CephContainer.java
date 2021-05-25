package com.clevercloud.testcontainers.ceph;

import org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;

public class CephContainer extends GenericContainer<CephContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("ceph/daemon");
    private static final String DEFAULT_TAG = "v6.0.3-stable-6.0-pacific-centos-8";
    private static final Integer CEPH_MON_DEFAULT_PORT = 3300;
    private static final Integer CEPH_RGW_DEFAULT_PORT = 8080;
    private static final String CEPH_DEMO_UID = "admin";
    private static final String CEPH_RGW_ACCESS_KEY = RandomStringUtils.randomAlphanumeric(32);
    private static final String CEPH_RGW_SECRET_KEY = RandomStringUtils.randomAlphanumeric(32);
    private static final String CEPH_END_START = ".*/entrypoint.sh: SUCCESS.*";
    private static final HashSet<String> CEPH_DEFAULT_DAEMONS = new HashSet<>(Collections.singletonList("all"));
    private static final String DEFAULT_RGW_NAME = "localhost";
    private static final Integer DEFAULT_RGW_HTTP_PORT = 8080;
    private static final String DEFAULT_RGW_PROTOCOL = "http";

    public CephContainer() {
        this(DEFAULT_TAG);
    }

    public CephContainer(final String tag) {
        this(DEFAULT_IMAGE_NAME.withTag(tag), CEPH_DEFAULT_DAEMONS);
    }

    public CephContainer(final String tag, HashSet<String> daemons) {
        this(DEFAULT_IMAGE_NAME.withTag(tag), daemons);
    }

    public CephContainer(final DockerImageName dockerImageName, HashSet<String> daemons) {
        super(dockerImageName);

        logger().info("Starting a Ceph container using [{}]", dockerImageName);
        addExposedPorts(CEPH_MON_DEFAULT_PORT, CEPH_RGW_DEFAULT_PORT);
        addEnv("DEMO_DEMONS", String.join(",", daemons));
        addEnv("CEPH_DEMO_UID", CEPH_DEMO_UID);
        addEnv("CEPH_DEMO_ACCESS_KEY", CEPH_RGW_ACCESS_KEY);
        addEnv("CEPH_DEMO_SECRET_KEY", CEPH_RGW_SECRET_KEY);
        addEnv("NETWORK_AUTO_DETECT", "1");
        addEnv("RGW_NAME", DEFAULT_RGW_NAME);
        setCommand("demo");

        setWaitStrategy(Wait.forLogMessage(CEPH_END_START, 1).withStartupTimeout(Duration.ofMinutes(5)));
    }

    public String getDefaultTag() {
        return DEFAULT_TAG;
    }

    public String getRGWHTTPHost() {
        return getHost();
    }

    public Integer getRGWHTTPPort() {
        return getMappedPort(DEFAULT_RGW_HTTP_PORT);
    }

    public String getRGWHTTPHostAddress() {
        return String.format("%s:%d", getRGWHTTPHost(), getRGWHTTPPort());
    }

    public String getRGWAccessKey() {
        return CEPH_RGW_ACCESS_KEY;
    }

    public String getRGWSecretKey() {
        return CEPH_RGW_SECRET_KEY;
    }

    public String getRGWUser() {
        return CEPH_DEMO_UID;
    }

    public URI getRGWUri() throws URISyntaxException {
        return new URI(String.format("%s://%s", getRGWProtocol(), getRGWHTTPHostAddress()));
    }

    public String getRGWProtocol() {
        return DEFAULT_RGW_PROTOCOL;
    }
}
