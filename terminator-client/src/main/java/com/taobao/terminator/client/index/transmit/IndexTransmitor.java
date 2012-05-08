package com.taobao.terminator.client.index.transmit;


/**
 * ����Դ���ݵĴ������
 * 
 * @author yusen
 */
public interface IndexTransmitor {
	public boolean start()throws IndexTransmitException;
	
	public boolean transmit(byte[] data) throws IndexTransmitException;
	
	public boolean finish() throws IndexTransmitException;
}
