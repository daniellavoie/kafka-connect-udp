package dev.daniellavoie.kafka.connect.udp;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;

public class UdpSourceConfiguration {
	public final static String UDP_SOURCE_GROUP = "UDP Source";

	public final static String UDP_SOURCE_PORT = "udp-source.port";
	public final static String UDP_SOURCE_PORT_DOC = "UDP source port";
	public final static String UDP_SOURCE_PORT_DISPLAY = "Port";

	public final static String UDP_SOURCE_MAX_PACKETS_PER_POLL = "udp-source.max-packets-per-poll";
	public final static String UDP_SOURCE_MAX_PACKETS_PER_POLL_DOC = "UDP source max packet per connect poll";
	public final static String UDP_SOURCE_MAX_PACKETS_PER_POLL_DISPLAY = "Max packet per connect poll";

	public final static String UDP_SOURCE_NAME = "udp-source.name";
	public final static String UDP_SOURCE_NAME_DOC = "UDP source name";
	public final static String UDP_SOURCE_NAME_DISPLAY = "Name";

	public final static String UDP_SOURCE_TOPIC = "udp-source.topic";
	public final static String UDP_SOURCE_TOPIC_DOC = "UDP source destination topic";
	public final static String UDP_SOURCE_TOPIC_DISPLAY = "Destination topic";

	private final int port;
	private final int maxPacketsPerPoll;
	private final String sourceName;
	private final String topic;

	public UdpSourceConfiguration(int port, int maxPacketsPerPoll, String sourceName, String topic) {
		this.port = port;
		this.maxPacketsPerPoll = maxPacketsPerPoll;
		this.sourceName = sourceName;
		this.topic = topic;
	}

	public int getPort() {
		return port;
	}

	public int getMaxPacketsPerPoll() {
		return maxPacketsPerPoll;
	}

	public String getSourceName() {
		return sourceName;
	}

	public String getTopic() {
		return topic;
	}

	public static UdpSourceConfiguration build(Map<String, String> props) {
		int port = getIntProp(UDP_SOURCE_PORT, props);
		int maxPacketsPerPoll = getIntProp(UDP_SOURCE_MAX_PACKETS_PER_POLL, props, "1000");

		String sourceName = getProp(UdpSourceConfiguration.UDP_SOURCE_NAME, props, String.valueOf(port));
		String topic = getProp(UdpSourceConfiguration.UDP_SOURCE_TOPIC, props, "udp-connector");

		return new UdpSourceConfiguration(port, maxPacketsPerPoll, sourceName, topic);
	}

	private static String getProp(String propKey, Map<String, String> props, String defaultValue)
			throws ConnectException {
		String value = props.get(propKey);
		if (value == null && defaultValue == null) {
			throw new ConnectException(propKey + " must be defined.");
		}

		return value;
	}

	private static int getIntProp(String propKey, Map<String, String> props) {
		return getIntProp(propKey, props, null);
	}

	private static int getIntProp(String propKey, Map<String, String> props, String defaultValue)
			throws ConnectException {
		String value = getProp(propKey, props, defaultValue);
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException ex) {
			throw new ConnectException(propKey + "is not a valid value for " + propKey + ".");
		}
	}

	public static ConfigDef configDef() {
		int orderInGroup = 0;

		return new ConfigDef()
				.define(UDP_SOURCE_PORT, Type.INT, Importance.HIGH, UDP_SOURCE_PORT_DOC, UDP_SOURCE_GROUP,
						++orderInGroup, Width.SHORT, UDP_SOURCE_PORT_DISPLAY, Arrays.asList())
				.define(UDP_SOURCE_MAX_PACKETS_PER_POLL, Type.INT, Importance.LOW, UDP_SOURCE_MAX_PACKETS_PER_POLL_DOC,
						UDP_SOURCE_GROUP, ++orderInGroup, Width.MEDIUM, UDP_SOURCE_MAX_PACKETS_PER_POLL_DISPLAY,
						Arrays.asList())
				.define(UDP_SOURCE_NAME, Type.STRING, Importance.HIGH, UDP_SOURCE_NAME_DOC, UDP_SOURCE_GROUP,
						++orderInGroup, Width.SHORT, UDP_SOURCE_NAME_DISPLAY, Arrays.asList())
				.define(UDP_SOURCE_TOPIC, Type.STRING, Importance.HIGH, UDP_SOURCE_TOPIC_DOC, UDP_SOURCE_GROUP,
						++orderInGroup, Width.SHORT, UDP_SOURCE_TOPIC_DISPLAY, Arrays.asList());
	}
}
