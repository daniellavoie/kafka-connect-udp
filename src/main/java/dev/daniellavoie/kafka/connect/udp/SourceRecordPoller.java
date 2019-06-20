package dev.daniellavoie.kafka.connect.udp;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.socket.DatagramPacket;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public class SourceRecordPoller {
	private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecordPoller.class);

	private final UdpSourceConfiguration udpSourceConfiguration;

	private final Queue<SourceRecord> bufferedRecords = new ConcurrentLinkedQueue<SourceRecord>();
	private final Schema schema;
	private final Disposable subscription;

	private UdpServer udpServer;

	public SourceRecordPoller(UdpSourceConfiguration udpSourceConfiguration) {
		this.udpSourceConfiguration = udpSourceConfiguration;

		schema = SchemaBuilder.struct().name("udpPacket").field("host", Schema.STRING_SCHEMA)
				.field("port", Schema.INT32_SCHEMA).field("sourceName", Schema.STRING_SCHEMA)
				.field("payload", Schema.BYTES_SCHEMA).build();

		subscription = Flux.<DatagramPacket>create(sink -> initializeUdpConnection(sink))

				.map(this::buildSourceRecord)

				.doOnNext(bufferedRecords::add)

				.doOnError(ex -> LOGGER.error("Unexpected error while processing udp source.", ex))

				.retryBackoff(Long.MAX_VALUE, Duration.ofSeconds(1), Duration.ofMinutes(5))

				.subscribe();
	}

	private SourceRecord buildSourceRecord(DatagramPacket packet) {
		Struct record = new Struct(schema);

		record.put("host", packet.sender().getHostString());
		record.put("port", udpSourceConfiguration.getPort());
		record.put("sourceName", udpSourceConfiguration.getSourceName());
		record.put("payload", ByteBufUtil.getBytes(packet.content()));

		Map<String, ?> partition = Stream.of(new AbstractMap.SimpleEntry<>("port", udpSourceConfiguration.getPort()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		return new SourceRecord(partition, null, udpSourceConfiguration.getTopic(), record.schema(), record);
	}

	private void initializeUdpConnection(FluxSink<DatagramPacket> sink) {
		udpServer = new UdpServer(sink, udpSourceConfiguration.getPort());
	}

	public List<SourceRecord> poll() {
		List<SourceRecord> records = new LinkedList<SourceRecord>();

		while (!bufferedRecords.isEmpty() && udpSourceConfiguration.getMaxPacketsPerPoll() > records.size()) {
			records.add(bufferedRecords.poll());
		}

		if (records.size() > 0) {
			LOGGER.info("Polled {} records from UDP connector.", records.size());
		}

		return records;
	}

	public void shutdown() {
		if (!subscription.isDisposed()) {
			subscription.dispose();
		} else {
			LOGGER.warn("UDP record poller has already been shutdown.");
		}

		udpServer.close();
	}
}
