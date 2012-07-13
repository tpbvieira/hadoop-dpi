package jxta.jnetpcap.socket;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import net.jxta.endpoint.Message;
import net.jxta.endpoint.MessageElement;
import net.jxta.impl.util.pipe.reliable.Defs;

import org.jnetpcap.packet.JPacket;
import org.jnetpcap.protocol.tcpip.Jxta;

/**
 * Class to represent a flow of a JXTA socket 
 * 
 * @author Thiago Vieira
 *
 */

public class JxtaSocketFlow {
	private boolean isEearlyClosed;

	private boolean isDataComplete;
	private int dataFlowId;	
	private short dataNumber;

	private boolean isAckComplete;
	private int ackFlowId;	
	private short ackNumber;

	private short expectedNumber;

	private SortedMap<Integer, Long[]> rtts;

	private long startTime = Long.MAX_VALUE;
	private long endTime = Long.MIN_VALUE;
	
	private long socketReqTime;
	private long socketRemTime;

	public JxtaSocketFlow(int dataFlowId, int ackFlowId){
		this.dataFlowId = dataFlowId;
		this.ackFlowId = ackFlowId;
		isDataComplete = false;
		isAckComplete = false;		
		isEearlyClosed = false;
		rtts = new TreeMap<Integer, Long[]>();
		dataNumber = 0;
		ackNumber = 0;
		expectedNumber = 3;// req + rely + close
		socketReqTime = 0;
		socketRemTime = 0;
	}
	
	public JxtaSocketFlow(int dataFlowId, int ackFlowId, long socketReqTime, long socketRemTime){
		this.dataFlowId = dataFlowId;
		this.ackFlowId = ackFlowId;
		isDataComplete = false;
		isAckComplete = false;		
		isEearlyClosed = false;
		rtts = new TreeMap<Integer, Long[]>();
		dataNumber = 0;
		ackNumber = 0;
		expectedNumber = 3;// req + rely + close
		this.socketReqTime = socketReqTime;
		this.socketRemTime = socketRemTime;
	}

	public boolean isDataComplete() {
		return isDataComplete;
	}

	public boolean isAckComplete() {
		return isAckComplete;
	}

	public int getDataFlowId() {
		return dataFlowId;
	}

	public int getAckFlowId() {
		return ackFlowId;
	}

	public SortedMap<Integer, Long[]> getRtts(){
		return rtts;
	}

	public long getStartTime(){
		return startTime;
	}

	public long getEndTime(){
		return endTime;
	}

	public long getTransferTime() {
		return endTime - startTime;
	}
	
	public long getSocketReqTime() {
		return socketReqTime;
	}

	public long getSocketRemTime() {
		return socketRemTime;
	}

	public void addJxtaData(Jxta newJxta) {		
		if(!isEearlyClosed){
			dataAdd(newJxta);
		}
		updateFlow();
	}

	public void addJxtaAck(Jxta newJxta) {		
		if(!isEearlyClosed){
			ackAdd(newJxta);
		}	
		updateFlow();
	}

	private void updateFlow(){
		if(dataNumber >= expectedNumber){
			isDataComplete = true;				
		}
		if(ackNumber >= expectedNumber){
			isAckComplete = true;
		}

		if(isDataComplete && isAckComplete && !isEearlyClosed){
			isEearlyClosed = true;
		}else
			if(isDataComplete && isAckComplete && isEearlyClosed){
				isEearlyClosed = false;
			}
	}

	/**
	 *  for each JXTA message get the payload and the RTT(first and last time)
	 */
	private void dataAdd(Jxta jxta){
		long tmp = 0;		

		SortedMap<Long,JPacket> pkts = jxta.getJxtaPackets();		
		if(pkts != null && pkts.size() > 0){			
			JPacket pkt = pkts.get(pkts.firstKey());
			tmp = pkt.getCaptureHeader().timestampInMillis();
			if(tmp < startTime)
				startTime = tmp;				
			pkt = pkts.get(pkts.lastKey());
			tmp = pkt.getCaptureHeader().timestampInMillis();
			if(tmp > endTime)
				endTime = tmp;			
		}else{
			tmp = jxta.getPacket().getCaptureHeader().timestampInMillis();				
			if(tmp < startTime)
				startTime = tmp;				
			if(tmp > endTime)
				endTime = tmp;
		}

		Iterator<MessageElement> it = jxta.getMessage().getMessageElements(Defs.NAMESPACE, Defs.MIME_TYPE_BLOCK);
		if(it != null && it.hasNext()){
			while(it.hasNext()){											
				MessageElement el = it.next();
				Integer seq = Integer.valueOf(el.getElementName());
				if((seq + 2) > expectedNumber)
					expectedNumber = (short) (seq.shortValue() + (short)2);
				Long[] rtt = new Long[2];
				rtt[0] = endTime;
				rtts.put(seq,rtt);
				dataNumber++;
			}
		}
	}

	private void ackAdd(Jxta ackJxta){		

		Message msg = ackJxta.getMessage();			
		MessageElement ackElement = msg.getMessageElement(Defs.ACK_ELEMENT_NAME);		

		if(ackElement != null){			
			long msgTime = 0;
			Long rtt[];
			JPacket packet;
			int sackNumber = ((int) ackElement.getByteLength() / 4) - 1;
			try {
				DataInputStream dis = new DataInputStream(ackElement.getStream());
				SortedMap<Long,JPacket> pkts = ackJxta.getJxtaPackets();
				
				if(pkts != null && pkts.size() > 0){						
					packet = pkts.get(pkts.lastKey());
				}else{
					packet = ackJxta.getPacket();	
				}
				msgTime = packet.getCaptureHeader().timestampInMillis();
				
				// iterate over the message ack number and sackNumber (sackNumber + this message)
				for (int sac = 0; sac < sackNumber + 1; sac++) {
					int seqAck = dis.readInt();
					if(rtts.containsKey(seqAck)){
						rtt = rtts.get(seqAck);
						if(rtt[1] == null)
							ackNumber++;
						rtt[1] = msgTime;
						rtts.put(seqAck, rtt);	
					}					
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public String toString(){		
		StringBuilder str = new StringBuilder();

		str.append("Data: ");
		str.append(isDataComplete);
		str.append(":");
		str.append(dataNumber);
		str.append('\n');

		str.append("Ack: ");
		str.append(isAckComplete);
		str.append(":");
		str.append(ackNumber);
		str.append('\n');

		str.append("ExpectedNumber: ");
		str.append(expectedNumber);

		return str.toString();
	}
}