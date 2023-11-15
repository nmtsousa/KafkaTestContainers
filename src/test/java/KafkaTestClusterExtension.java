import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.extension.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

public class KafkaTestClusterExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private final Network network = Network.newNetwork();

    private final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.2"))
            .withNetwork(network);

    private final GenericContainer<?> schemaRegistry = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.5.2"))
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                    "PLAINTEXT://" + kafkaContainer.getNetworkAliases().get(0) + ":9092")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @Override
    public void beforeAll(ExtensionContext context) {
        kafkaContainer.start();
        schemaRegistry.start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        schemaRegistry.start();
        kafkaContainer.stop();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType()
                .equals(KafkaTestCluster.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return (KafkaTestCluster) () -> {
            Properties kafkaSettings = new Properties();
            kafkaSettings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaContainer.getBootstrapServers());
            kafkaSettings.put("schema.registry.url",
                    "http://" + schemaRegistry.getHost() +
                            ":" + schemaRegistry.getFirstMappedPort());

            return kafkaSettings;
        };
    }

    public interface KafkaTestCluster {
        Properties getKafkaSettings();
    }
}
