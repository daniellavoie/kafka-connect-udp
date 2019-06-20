package dev.daniellavoie.kafka.connect.udp;

import java.time.Duration;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

public class UdpServerExample {

	public static void main(String[] args) {
		Connection server = UdpServer.create().handle((in, out) -> out.sendObject(in.receiveObject().map(o -> {
			if (o instanceof DatagramPacket) {
				DatagramPacket p = (DatagramPacket) o;
				ByteBuf buf = Unpooled.copiedBuffer("hello", CharsetUtil.UTF_8);
				return new DatagramPacket(buf, p.sender());
			} else {
				return Mono.error(new Exception("Unexpected type of the message: " + o));
			}
		}))).bindNow(Duration.ofMinutes(30));

		server.onDispose().block();
	}
}
