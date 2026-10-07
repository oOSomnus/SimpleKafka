package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.TempDirectory;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step10Test {
    @Test
    void createsFixedSortedMetadataAndReopensOnlyTheExplicitPartitionCount() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            List<PartitionMetadata> beforeRestart;
            try (BrokerHarness broker = BrokerHarness.single(temp.root(), 7, 0)) {
                PartitionCatalog catalog = broker.catalog();
                catalog.createTopic("orders", 3);
                catalog.createTopic("orders", 3);
                beforeRestart = catalog.metadata("orders");

                assertEquals(List.of(0, 1, 2), beforeRestart.stream().map(item -> item.tp().partition()).toList());
                for (int partition = 0; partition < 3; partition++) {
                    PartitionMetadata metadata = beforeRestart.get(partition);
                    assertEquals(new TopicPartition("orders", partition), metadata.tp());
                    assertEquals(7, metadata.leaderId());
                    assertEquals(0, metadata.epoch());
                    assertEquals(List.of(7), metadata.replicas());
                    assertEquals(List.of(7), metadata.isr());
                    assertEquals(broker.endpoint(), metadata.leader());
                }

                assertCode(ErrorCode.INVALID_REQUEST, () -> catalog.createTopic("orders", 4));
                assertCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, () -> catalog.metadata("missing"));
            }

            try (BrokerHarness restarted = BrokerHarness.single(temp.root(), 7, 0)) {
                restarted.catalog().createTopic("orders", 3);
                List<PartitionMetadata> afterRestart = restarted.catalog().metadata("orders");
                assertEquals(3, afterRestart.size());
                for (int partition = 0; partition < 3; partition++) {
                    PartitionMetadata metadata = afterRestart.get(partition);
                    assertEquals(beforeRestart.get(partition).tp(), metadata.tp());
                    assertEquals(beforeRestart.get(partition).leaderId(), metadata.leaderId());
                    assertEquals(beforeRestart.get(partition).epoch(), metadata.epoch());
                    assertEquals(beforeRestart.get(partition).replicas(), metadata.replicas());
                    assertEquals(beforeRestart.get(partition).isr(), metadata.isr());
                    assertEquals(restarted.endpoint(), metadata.leader());
                }
            }
        }
    }
}
