package org.taobao.terminator.client.multicast;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class Sender {
	public static void main(String[] args) throws Exception{
		int port = 12345;
		String sendMessage = "ÄãºÃ";
		InetAddress inetAddress = InetAddress.getByName("239.66.69.18");
		DatagramPacket datagramPacket = new DatagramPacket(sendMessage.getBytes(), sendMessage.length(), inetAddress, port);
		MulticastSocket multicastSocket = new MulticastSocket();
		multicastSocket.send(datagramPacket);
	}
}
