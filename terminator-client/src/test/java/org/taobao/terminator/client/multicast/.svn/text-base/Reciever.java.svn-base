package org.taobao.terminator.client.multicast;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class Reciever {
	public static void main(String[] args) throws Exception{
		InetAddress group = InetAddress.getByName("239.66.69.18");
		MulticastSocket s = new MulticastSocket(12345);
		byte[] arb = new byte[1024];
		s.joinGroup(group);
		while(true) {
			DatagramPacket packet = new DatagramPacket(arb, arb.length);
			s.receive(packet);
			System.out.println(arb.length);
			System.out.println(new String(arb));
		}
	}
}
