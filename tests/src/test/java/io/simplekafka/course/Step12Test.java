package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.record;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class Step12Test {
    @Test
    void servesMetadataProduceAndFetchOverLoopbackAndKeepsServingAfterDomainErrors() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient client = new RpcClient(broker.endpoint(), 3_000)) {
            TopicPartition tp = new TopicPartition("orders", 0);
            Messages.Reply metadataReply = client.call(new Messages.MetadataRequest("orders"));
            assertEquals(ErrorCode.NONE, metadataReply.error());
            Messages.MetadataBody metadata = assertInstanceOf(Messages.MetadataBody.class, metadataReply.body());
            assertEquals(List.of(tp), metadata.partitions().stream().map(item -> item.tp()).toList());

            Messages.Reply produced = client.call(new Messages.ProduceRequest(tp, 0, Acks.LEADER, 1_000,
                    List.of(record("k", "v0", 10), record("k", "v1", 11), record(null, "v2", 12))));
            assertEquals(ErrorCode.NONE, produced.error());
            assertEquals(new io.simplekafka.model.AppendResult(0, 3),
                    assertInstanceOf(Messages.ProduceBody.class, produced.body()).result());

            Messages.FetchBody fetched = TestSupport.fetch(client, tp, 0, 10, 4_096);
            assertEquals(List.of(0L, 1L, 2L), fetched.records().stream().map(LogRecord::offset).toList());
            assertEquals(List.of("v0", "v1", "v2"), TestSupport.values(fetched.records()));
            assertEquals(0, fetched.logStartOffset());
            assertEquals(3, fetched.logEndOffset());
            assertEquals(3, fetched.highWatermark());

            Messages.Reply fenced = client.call(new Messages.ProduceRequest(tp, 1, Acks.LEADER, 1_000,
                    List.of(record(null, "must-not-append", 13))));
            assertEquals(ErrorCode.FENCED_EPOCH, fenced.error());
            Messages.Reply unknown = client.call(new Messages.MetadataRequest("missing"));
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, unknown.error());
            Messages.Reply outOfRange = client.call(new Messages.FetchRequest(tp, 0, 4, 1, 256, null));
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, outOfRange.error());

            assertEquals(List.of("v0", "v1", "v2"), TestSupport.values(TestSupport.fetch(client, tp, 0, 10, 4_096).records()),
                    "failed requests must not terminate the broker or mutate the log");
        }
    }
}
