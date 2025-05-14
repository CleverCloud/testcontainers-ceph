package com.clevercloud.testcontainers.ceph;

import org.testcontainers.containers.GenericContainer;

import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CephContainer extends GenericContainer<CephContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("clevercloud/testcontainer-ceph");
    private static final String DEFAULT_TAG = "reef-20250513";
    private static final Integer CEPH_MON_DEFAULT_PORT = 3300;
    private static final Integer CEPH_RGW_DEFAULT_PORT = 7480;
    private static final Integer MGR_PORT = 8080;
    private static final String CEPH_DEMO_UID = "admin";
    private static final String CEPH_RGW_ACCESS_KEY = "radosgwadmin";
    private static final String CEPH_RGW_SECRET_KEY = "radosgwadmin";
    private static final HashSet<String> CEPH_DEFAULT_FEATURES = new HashSet<>(
            Collections.singletonList("radosgw rbd"));
    private static final String CEPH_RGW_PROTOCOL = "http";

    private static final String MGR_USERNAME = "admin";
    private static final String MGR_PASSWORD = "admin";

    public CephContainer() {
        this(DEFAULT_TAG);
    }

    public CephContainer(final String tag) {
        this(DEFAULT_IMAGE_NAME.withTag(tag), CEPH_DEFAULT_FEATURES);
    }

    public CephContainer(final String tag, HashSet<String> features) {
        this(DEFAULT_IMAGE_NAME.withTag(tag), features);
    }

    private static final Logger log = LoggerFactory.getLogger(CephContainer.class);

    public CephContainer(final DockerImageName dockerImageName, HashSet<String> features) {
        super(dockerImageName);

        log.info("Starting a Ceph container using [{}]", dockerImageName);

        addExposedPorts(CEPH_MON_DEFAULT_PORT, CEPH_RGW_DEFAULT_PORT, MGR_PORT);
        addEnv("FEATURES", String.join(" ", features));
        addEnv("ACCESS_KEY", CEPH_RGW_ACCESS_KEY);
        addEnv("SECRET_KEY", CEPH_RGW_SECRET_KEY);
        addEnv("MGR_USERNAME", MGR_USERNAME);
        addEnv("MGR_PASSWORD", MGR_PASSWORD);
        addEnv("NETWORK_AUTO_DETECT", "1");

        setWaitStrategy(new DashboardApiWaitStrategy()
                .withPort(MGR_PORT)
                .withUsername(MGR_USERNAME)
                .withPassword(MGR_PASSWORD)
                .withTimeout(Duration.ofMinutes(2)));
    }

    public String getDashboardApiUrl() {
        return String.format("http://%s:%d/api", getHost(), getMappedPort(MGR_PORT));
    }

    public String getDefaultTag() {
        return DEFAULT_TAG;
    }

    public String getRGWHTTPHost() {
        return getHost();
    }

    public Integer getRGWHTTPPort() {
        return getMappedPort(CEPH_RGW_DEFAULT_PORT);
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
        return CEPH_RGW_PROTOCOL;
    }
}
