package dev.daniellavoie.kafka.connect.udp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import reactor.core.publisher.FluxSink;

public class UdpChannelHandler extends SimpleChannelInboundHandler<DatagramPacket> {
	private static final Logger LOGGER = LoggerFactory.getLogger(UdpChannelHandler.class);

	private FluxSink<DatagramPacket> sink;

	public UdpChannelHandler(FluxSink<DatagramPacket> sink) {
		this.sink = sink;
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
		LOGGER.trace("Received a packet from {}:{}.", packet.sender().getHostString(), packet.sender().getPort());
		
		sink.next(packet);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		ctx.close();

		sink.error(cause);
	}
}
