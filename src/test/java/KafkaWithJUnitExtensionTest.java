import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;
import java.util.Set;

import static org.testcontainers.shaded.org.hamcrest.CoreMatchers.equalTo;
import static org.testcontainers.shaded.org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(KafkaTestClusterExtension.class)
class KafkaWithJUnitExtensionTest {

    @Test
    void clusterStartsWithSchemasTopic(KafkaTestCluster cluster) throws Exception {

        Properties kafkaSettings = cluster.getKafkaSettings();

        // Run the test

        try (Admin admin = Admin.create(kafkaSettings)) {
            ListTopicsResult listTopicsResult = admin.listTopics();
            Set<String> names = listTopicsResult.names().get();
            assertThat(names.size(), equalTo(1));
            assertThat(names.iterator().next(), equalTo("_schemas"));
        }
    }
}
