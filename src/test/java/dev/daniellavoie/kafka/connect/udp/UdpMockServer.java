package dev.daniellavoie.kafka.connect.udp;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.SocketUtils;

public class UdpMockServer implements Closeable {
	private static final Logger LOGGER = LoggerFactory.getLogger(UdpMockServer.class);

	private final int broadcastPort;
	private final Channel channel;
	private final EventLoopGroup group;

	public UdpMockServer(int broadcastPort) throws IOException {
		this.broadcastPort = broadcastPort;
		this.group = new NioEventLoopGroup();

		try {
			Bootstrap b = new Bootstrap();

			b.group(group).channel(NioDatagramChannel.class).option(ChannelOption.SO_BROADCAST, true)
					.handler(new UdpMockServerHandler());

			channel = b.bind(0).sync().channel();

			LOGGER.info("UDP Mock server initialization completed.");
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		channel.close();
	}

	public void sendData(byte[] bytes) {
		try {
			channel.writeAndFlush(new DatagramPacket(Unpooled.copiedBuffer("QOTM?", CharsetUtil.UTF_8),
					SocketUtils.socketAddress("255.255.255.255", broadcastPort))).sync();

			LOGGER.info("Broacasted mocked data to port {}.", broadcastPort);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	class UdpMockServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {
		public void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
			String response = msg.content().toString(CharsetUtil.UTF_8);
			if (response.startsWith("QOTM: ")) {
				System.out.println("Quote of the Moment: " + response.substring(6));
				ctx.close();
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			cause.printStackTrace();
			ctx.close();
		}
	}
}
