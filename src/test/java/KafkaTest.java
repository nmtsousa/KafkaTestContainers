import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.hamcrest.CoreMatchers;
import org.testcontainers.shaded.org.hamcrest.MatcherAssert;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.Set;

import static org.testcontainers.shaded.org.hamcrest.CoreMatchers.equalTo;
import static org.testcontainers.shaded.org.hamcrest.MatcherAssert.assertThat;

@Testcontainers
class KafkaTest {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.2"))
            .withNetwork(NETWORK);

    @Container
    private static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.5.2"))
            .withNetwork(NETWORK)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                    "PLAINTEXT://" + KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @Test
    void testListTopics() throws Exception {

        Properties kafkaSettings = new Properties();
        kafkaSettings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        kafkaSettings.put("schema.registry.url",
                "http://" + SCHEMA_REGISTRY.getHost() +
                        ":" + SCHEMA_REGISTRY.getFirstMappedPort());

        // Remaining KAFKA Settings

        try (Admin admin = Admin.create(kafkaSettings)) {
            ListTopicsResult listTopicsResult = admin.listTopics();
            Set<String> names = listTopicsResult.names().get();
            assertThat(names.size(), equalTo(1));
            assertThat(names.iterator().next(), equalTo("_schemas"));
        }
    }
}
