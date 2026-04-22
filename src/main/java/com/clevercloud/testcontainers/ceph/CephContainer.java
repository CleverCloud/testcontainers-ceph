package com.clevercloud.testcontainers.ceph;

import org.testcontainers.containers.GenericContainer;

import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class CephContainer extends GenericContainer<CephContainer> {
    public enum Track {
        PACIFIC,
        REEF;

        public String prefix() {
            return name().toLowerCase();
        }
    }

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("clevercloud/testcontainer-ceph");
    private static final Track DEFAULT_TRACK = Track.REEF;
    private static final String DEFAULT_DATE;

    static {
        try (InputStream is = CephContainer.class.getResourceAsStream("/ceph-testcontainer.properties")) {
            if (is == null) {
                throw new ExceptionInInitializerError("ceph-testcontainer.properties missing from classpath");
            }
            Properties props = new Properties();
            props.load(is);
            DEFAULT_DATE = props.getProperty("default.date");
            if (DEFAULT_DATE == null || DEFAULT_DATE.isEmpty() || DEFAULT_DATE.contains("${")) {
                throw new ExceptionInInitializerError("default.date not resolved (resource filtering disabled?)");
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final Integer CEPH_MON_DEFAULT_PORT = 3300;
    private static final Integer CEPH_RGW_DEFAULT_PORT = 7480;
    private static final Integer MGR_PORT = 8080;
    private static final String CEPH_DEMO_UID = "admin";
    private static final String CEPH_RGW_ACCESS_KEY = "radosgwadmin";
    private static final String CEPH_RGW_SECRET_KEY = "radosgwadmin";
    private static final HashSet<String> CEPH_DEFAULT_FEATURES = new HashSet<>(
            Collections.singletonList("radosgw rbd mon"));
    private static final String CEPH_RGW_PROTOCOL = "http";

    private static final String MGR_USERNAME = "admin";
    private static final String MGR_PASSWORD = "admin";

    public CephContainer() {
        this(DEFAULT_TRACK, DEFAULT_DATE);
    }

    public CephContainer(Track track) {
        this(track, DEFAULT_DATE);
    }

    public CephContainer(Track track, String date) {
        this(track.prefix() + "-" + date);
    }

    public CephContainer(Track track, String date, HashSet<String> features) {
        this(track.prefix() + "-" + date, features);
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

        // Log all container output for debugging
        withLogConsumer(new Slf4jLogConsumer(log).withPrefix("CEPH"));
    }

    public String getDashboardUsername() {
        return MGR_USERNAME;
    }

    public String getDashboardPassword() {
        return MGR_PASSWORD;
    }

    public String getDashboardApiUrl() {
        return String.format("http://%s:%d/api", getHost(), getMappedPort(MGR_PORT));
    }

    public String getDefaultTag() {
        return defaultTag();
    }

    public static String defaultTag() {
        return DEFAULT_TRACK.prefix() + "-" + DEFAULT_DATE;
    }

    public static String defaultDate() {
        return DEFAULT_DATE;
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

    public Integer getMONPort() {
        return getMappedPort(CEPH_MON_DEFAULT_PORT);
    }

    /**
     * Returns the container's IP address from Docker network settings.
     * This returns the actual IP address, not localhost.
     *
     * @return the container's IP address
     */
    public String getContainerIpAddress() {
        return getContainerInfo().getNetworkSettings().getIpAddress();
    }

    /**
     * Initializes a pool for RBD usage via the dashboard API
     * (POST /api/pool/{pool_name}/init — backported from upstream).
     *
     * @param poolName pool to initialize
     * @param force    re-initialize even if already initialized
     * @return HTTP response code from the dashboard
     */
    public int initPool(String poolName, boolean force) {
        try {
            String token = dashboardAuthToken();
            URL url = new URI(String.format("%s/pool/%s/init", getDashboardApiUrl(), poolName)).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Accept", "application/vnd.ceph.api.v1.0+json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setDoOutput(true);
            byte[] body = String.format("{\"force\": %b}", force).getBytes(StandardCharsets.UTF_8);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }
            int code = conn.getResponseCode();
            log.info("initPool({}, force={}) -> HTTP {}", poolName, force, code);
            return code;
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to init pool " + poolName, e);
        }
    }

    private String dashboardAuthToken() throws IOException, URISyntaxException {
        URL url = new URI(getDashboardApiUrl() + "/auth").toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Accept", "application/vnd.ceph.api.v1.0+json");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        byte[] body = String.format("{\"username\":\"%s\",\"password\":\"%s\"}", MGR_USERNAME, MGR_PASSWORD)
                .getBytes(StandardCharsets.UTF_8);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body);
        }
        if (conn.getResponseCode() != 201) {
            throw new IOException("Dashboard auth failed: HTTP " + conn.getResponseCode());
        }
        String resp = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int idx = resp.indexOf("\"token\"");
        int start = resp.indexOf('"', resp.indexOf(':', idx) + 1) + 1;
        int end = resp.indexOf('"', start);
        return resp.substring(start, end);
    }

    /**
     * Retrieves the Ceph cluster ID (FSID) from the running container.
     * The cluster ID is a unique identifier for the Ceph cluster.
     *
     * @return the Ceph cluster ID as a String
     * @throws RuntimeException if the cluster ID cannot be retrieved
     */
    public String getClusterId() {
        try {
            // Execute ceph fsid command to get the cluster ID
            org.testcontainers.containers.Container.ExecResult result = execInContainer("ceph", "fsid");

            if (result.getExitCode() != 0) {
                throw new RuntimeException("Failed to retrieve cluster ID. Exit code: " + result.getExitCode() +
                        ", stderr: " + result.getStderr());
            }

            String clusterId = result.getStdout().trim();
            if (clusterId.isEmpty()) {
                throw new RuntimeException("Cluster ID is empty");
            }

            log.debug("Retrieved Ceph cluster ID: {}", clusterId);
            return clusterId;
        } catch (Exception e) {
            throw new RuntimeException("Error retrieving Ceph cluster ID", e);
        }
    }
}
