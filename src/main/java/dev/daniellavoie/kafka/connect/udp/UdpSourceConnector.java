package dev.daniellavoie.kafka.connect.udp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class UdpSourceConnector extends SourceConnector {
	private static final ConfigDef CONFIG_DEF = UdpSourceConfiguration.configDef();

	private Map<String, String> props;

	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start(Map<String, String> props) {
		this.props = props;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return UdpSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		return IntStream.range(0, maxTasks).boxed().map(index -> new HashMap<>(props)).collect(Collectors.toList());
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

}
