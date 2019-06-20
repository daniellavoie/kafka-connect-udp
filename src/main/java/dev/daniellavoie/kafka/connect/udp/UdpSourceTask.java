package dev.daniellavoie.kafka.connect.udp;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class UdpSourceTask extends SourceTask {
	private SourceRecordPoller sourceRecordPoller;

	public String version() {
		return "0.0.1";
	}

	@Override
	public void start(Map<String, String> props) {
		UdpSourceConfiguration udpSourceConfiguration = UdpSourceConfiguration.build(props);

		sourceRecordPoller = new SourceRecordPoller(udpSourceConfiguration);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		List<SourceRecord> records = sourceRecordPoller.poll();

		if (records.isEmpty()) {
			return null;
		}

		return records;
	}

	@Override
	public void stop() {
		sourceRecordPoller.shutdown();
	}

}
