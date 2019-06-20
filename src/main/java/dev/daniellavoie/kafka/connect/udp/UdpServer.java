package dev.daniellavoie.kafka.connect.udp;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import reactor.core.publisher.FluxSink;

public class UdpServer implements Closeable {
	private static final Logger LOGGER = LoggerFactory.getLogger(UdpServer.class);

	private final EventLoopGroup eventLoopGroup;

	public UdpServer(FluxSink<DatagramPacket> sink, int port) {
		eventLoopGroup = new NioEventLoopGroup();

		try {
			Bootstrap b = new Bootstrap();
			b.group(eventLoopGroup).channel(NioDatagramChannel.class).option(ChannelOption.SO_BROADCAST, true)
					.handler(new UdpChannelHandler(sink));

			b.bind(port).sync().channel();

			LOGGER.info("Port {} is now bound to UDP Server.", port);
		} catch (InterruptedException e) {
			this.close();

			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		eventLoopGroup.shutdownGracefully();
	}

}
