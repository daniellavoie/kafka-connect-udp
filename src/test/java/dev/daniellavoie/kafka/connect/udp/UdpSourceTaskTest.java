package dev.daniellavoie.kafka.connect.udp;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

public class UdpSourceTaskTest {
	@Test
	public void canPoll() throws IOException, InterruptedException {
		ServerSocket socket = new ServerSocket(0);
		int broadcastPort = socket.getLocalPort();
		socket.close();

		UdpSourceTask task = new UdpSourceTask();
		try (UdpMockServer server = new UdpMockServer(broadcastPort)) {

			Map<String, String> props = new HashMap<>();

			props.put(UdpSourceConfiguration.UDP_SOURCE_PORT, String.valueOf(broadcastPort));
			props.put(UdpSourceConfiguration.UDP_SOURCE_MAX_PACKETS_PER_POLL, String.valueOf(100));
			props.put(UdpSourceConfiguration.UDP_SOURCE_NAME, "udp-connector-test");
			props.put(UdpSourceConfiguration.UDP_SOURCE_TOPIC, "udp-connector-test-topic");

			task.start(props);

			server.sendData("Hello".getBytes());

			Thread.sleep(2000);

			List<SourceRecord> records = task.poll();

			Assert.assertEquals(1, records.size());

			records = task.poll();

			Assert.assertNull(records);

			server.sendData("Hello".getBytes());

			Thread.sleep(2000);

			records = task.poll();

			Assert.assertEquals(1, records.size());
		}finally {
			task.stop();
		}
	}
}
