package jxta.jnetpcap.socket;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import net.jxta.document.AdvertisementFactory;
import net.jxta.document.StructuredDocumentFactory;
import net.jxta.document.XMLDocument;
import net.jxta.endpoint.Message;
import net.jxta.endpoint.Message.ElementIterator;
import net.jxta.endpoint.MessageElement;
import net.jxta.impl.endpoint.router.EndpointRouterMessage;
import net.jxta.impl.util.pipe.reliable.Defs;
import net.jxta.parser.exceptions.JxtaHeaderParserException;
import net.jxta.protocol.PipeAdvertisement;

import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Jxta;
import org.jnetpcap.protocol.tcpip.Jxta.JxtaMessageType;
import org.jnetpcap.protocol.tcpip.JxtaUtils;
import org.jnetpcap.protocol.tcpip.Tcp;

/**
 * Class to represent a flow of a JXTA Socket and provide some utilities 
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

	private static final String socketNamespace = "JXTASOC";
	private static final String reqPipeTag = "reqPipe";
	private static final String remPipeTag = "remPipe";	
	private static final String closeTag = "close";

	public static boolean isDebug = false;

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

	public static void generateSocketFlows(final StringBuilder errbuf, final Pcap pcap, final SortedMap<Integer,JxtaSocketFlow> dataFlows,
			final SortedMap<Integer,JxtaSocketFlow> ackFlows) {

		final SortedMap<Integer,Jxta> fragments = new TreeMap<Integer,Jxta>();		
		final SortedMap<Integer,Jxta> reqPendings = new TreeMap<Integer,Jxta>();
		final SortedMap<Integer,ArrayList<Jxta>> pipePendings = new TreeMap<Integer,ArrayList<Jxta>>();

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
			Tcp tcp = new Tcp();
			Jxta jxta = new Jxta();

			public void nextPacket(JPacket packet, StringBuilder errbuf) {

//				if(packet.getFrameNumber() == 19114)
//					System.out.println("### Frame Found! " + packet.getFrameNumber());

				if(packet.hasHeader(Tcp.ID)){
					packet.getHeader(tcp);
					long seqNumber = tcp.seq();

					// Looking for jxta fragmentation for this flowId(IP + port(source/dest)) 
					if(fragments.size() > 0 && tcp.getPayloadLength() > 0){
						Ip4 ip = new Ip4();
						packet.getHeader(ip);
						int tcpFlowId = JxtaUtils.getFlowId(ip,tcp);
						jxta = fragments.get(tcpFlowId);

						if(jxta != null){
							// writes actual payload into last payload
							ByteArrayOutputStream buffer = new ByteArrayOutputStream();
							buffer.write(jxta.getJxtaPayload(), 0, jxta.getJxtaPayload().length);					
							buffer.write(tcp.getPayload(), 0, tcp.getPayload().length);
							ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.toByteArray());
							try{
								jxta.decode(seqNumber,packet,byteBuffer);								
								updateSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
								fragments.remove(tcpFlowId);
								if(byteBuffer.hasRemaining()){
									try{
										jxta = new Jxta();
										packet.getHeader(jxta);
										byte[] rest = new byte[byteBuffer.remaining()];
										byteBuffer.get(rest, 0, byteBuffer.remaining());
										byteBuffer = ByteBuffer.wrap(rest);
										jxta.decode(seqNumber,packet,byteBuffer);										
										updateSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
										if(byteBuffer.hasRemaining()){											
											jxta = new Jxta();
											packet.getHeader(jxta);
											byte[] rest2 = new byte[byteBuffer.remaining()];
											byteBuffer.get(rest2, 0, byteBuffer.remaining());
											jxta.decode(seqNumber,packet,ByteBuffer.wrap(rest2));										
											updateSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
											return;
										}
									}catch(BufferUnderflowException e ){
										SortedMap<Long,JPacket> packets = jxta.getJxtaPackets();
										packets.clear();
										packets.put(seqNumber,packet);
										fragments.put(tcpFlowId,jxta);										
									}catch (IOException failed) {
										SortedMap<Long,JPacket> packets = jxta.getJxtaPackets();
										packets.clear();
										packets.put(seqNumber,packet);
										fragments.put(tcpFlowId,jxta);
									}catch (JxtaHeaderParserException e) {
										SortedMap<Long,JPacket> packets = jxta.getJxtaPackets();
										packets.clear();
										packets.put(seqNumber,packet);
										fragments.put(tcpFlowId,jxta);
									}catch (Exception e) {
										e.printStackTrace();
									}
									return;
								}
							}catch(BufferUnderflowException e ){								
								jxta.getJxtaPackets().put(seqNumber,packet);
								fragments.put(tcpFlowId, jxta);
							}catch (IOException failed) {
								jxta.getJxtaPackets().put(seqNumber,packet);
								fragments.put(tcpFlowId, jxta);
							}catch (Exception e) {
								e.printStackTrace();
							}
							return;
						}
					}

					if (packet.hasHeader(Jxta.ID)) {						
						jxta = new Jxta();
						packet.getHeader(jxta);
						if(jxta.getJxtaMessageType() == JxtaMessageType.DEFAULT){
							try{									
								jxta.decodeMessage();								
								updateSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
								if(jxta.isFragmented()){									
									ByteBuffer remain = ByteBuffer.wrap(jxta.getRemain());									
									jxta = new Jxta();
									packet.getHeader(jxta);
									jxta.decode(remain);
									jxta.getJxtaPackets().put(seqNumber,packet);
									updateSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
									if(jxta.isFragmented()){									
										remain = ByteBuffer.wrap(jxta.getRemain());									
										jxta = new Jxta();
										packet.getHeader(jxta);
										jxta.decode(remain);
										updateSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
									}
								}
							}catch(BufferUnderflowException e ){
								Ip4 ip = new Ip4();
								packet.getHeader(ip);
								int id = JxtaUtils.getFlowId(ip,tcp);								
								jxta.setFragmented(true);
								jxta.getJxtaPackets().put(seqNumber,packet);
								fragments.put(id,jxta);
							}catch(IOException e){
								Ip4 ip = new Ip4();
								packet.getHeader(ip);
								int id = JxtaUtils.getFlowId(ip,tcp);	
								jxta.setFragmented(true);
								jxta.getJxtaPackets().put(seqNumber,packet);
								fragments.put(id,jxta);
							}catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			}

		}, errbuf);

		if(fragments.size() > 0 && isDebug)
			System.out.println("### Fragments: " + fragments.size());

		if(reqPendings.size() > 0 && isDebug)
			System.out.println("### ReqPipe Pendings: " + reqPendings.size());

		if(pipePendings.size() > 0 && isDebug)
			System.out.println("### PipePendings: " + pipePendings.size());
	}

	@SuppressWarnings("rawtypes")
	private static void updateSocketFlows(Jxta jxta, SortedMap<Integer,Jxta> reqPendings, SortedMap<Integer,ArrayList<Jxta>> pipePendings, SortedMap<Integer,JxtaSocketFlow> dataFlows, SortedMap<Integer,JxtaSocketFlow> ackFlows){
		MessageElement el = null;
		ElementIterator elements;
		Message msg = jxta.getMessage();

		try{
			elements = msg.getMessageElements(Defs.NAMESPACE, Defs.MIME_TYPE_BLOCK);// jxta-reliable-block
			if(elements != null && elements.hasNext()){
				EndpointRouterMessage erm = new EndpointRouterMessage(msg,false);
				String strPipeId = erm.getDestAddress().getServiceParameter();
				Integer dataPipeId = Integer.valueOf(strPipeId.hashCode());

				if(dataFlows.containsKey(dataPipeId)){
					dataFlows.get(dataPipeId).addJxtaData(jxta);
				}else{
					if(pipePendings.containsKey(dataPipeId)){
						pipePendings.get(dataPipeId).add(jxta);
					}else{
						ArrayList<Jxta> jxtaList = new ArrayList<Jxta>();
						jxtaList.add(jxta);
						pipePendings.put(dataPipeId, jxtaList);
					}
				}
			}else{
				el = msg.getMessageElement(Defs.ACK_ELEMENT_NAME);
				if(el != null){
					EndpointRouterMessage erm = new EndpointRouterMessage(msg,false);
					String strPipeId = erm.getDestAddress().getServiceParameter();
					Integer ackPipeId = Integer.valueOf(strPipeId.hashCode());

					if(ackFlows.containsKey(ackPipeId)){
						ackFlows.get(ackPipeId).addJxtaAck(jxta);
					}else{
						if(pipePendings.containsKey(ackPipeId)){
							pipePendings.get(ackPipeId).add(jxta);
						}else{
							ArrayList<Jxta> jxtaList = new ArrayList<Jxta>();
							jxtaList.add(jxta);
							pipePendings.put(ackPipeId, jxtaList);
						}
					}
				}else{
					el = msg.getMessageElement(socketNamespace, closeTag);
					if(el != null){
						EndpointRouterMessage erm = new EndpointRouterMessage(msg,false);
						String strPipeId = erm.getDestAddress().getServiceParameter();
						Integer pipeId = Integer.valueOf(strPipeId.hashCode());

						if(dataFlows.containsKey(pipeId)){
							dataFlows.get(pipeId).addJxtaData(jxta);
						}else
							if(ackFlows.containsKey(pipeId)){
								ackFlows.get(pipeId).addJxtaAck(jxta);
							}else{
								if(pipePendings.containsKey(pipeId)){
									pipePendings.get(pipeId).add(jxta);
								}else{
									ArrayList<Jxta> jxtaList = new ArrayList<Jxta>();
									jxtaList.add(jxta);
									pipePendings.put(pipeId, jxtaList);
								}
							}
					}else{
						el = msg.getMessageElement(socketNamespace, reqPipeTag);
						if(el != null){
							XMLDocument advertisement = (XMLDocument) StructuredDocumentFactory.newStructuredDocument(el);
							PipeAdvertisement pipeAdv = (PipeAdvertisement) AdvertisementFactory.newAdvertisement(advertisement);					
							String strPipeId = new String(pipeAdv.getID().toString());
							int ackPipeId = strPipeId.hashCode();
							reqPendings.put(ackPipeId, jxta);
						}else{
							el = msg.getMessageElement(socketNamespace, remPipeTag);
							if (el != null) {
								XMLDocument adv = (XMLDocument) StructuredDocumentFactory.newStructuredDocument(el);
								PipeAdvertisement pipeAdv = (PipeAdvertisement) AdvertisementFactory.newAdvertisement(adv);

								EndpointRouterMessage erm = new EndpointRouterMessage(msg,false);

								String strDataPipeId = new String(pipeAdv.getID().toString());//createdPipe
								String strAckPipeId = erm.getDestAddress().getServiceParameter();//usedPipe

								int dataPipeId = strDataPipeId.hashCode();
								int ackPipeId = strAckPipeId.hashCode();

								Jxta reqPipe = reqPendings.remove(ackPipeId);

								long socketReqTime = reqPipe.getPacket().getCaptureHeader().timestampInMillis();
								long socketRemTime = jxta.getPacket().getCaptureHeader().timestampInMillis(); 

								JxtaSocketFlow socketFlow = new JxtaSocketFlow(dataPipeId, ackPipeId, socketReqTime, socketRemTime);

								socketFlow.addJxtaData(reqPipe);
								socketFlow.addJxtaAck(jxta);

								//TODO test this implementation with iteration over key to verify if the performance is better
								if(pipePendings.containsKey(dataPipeId)){
									ArrayList<Jxta> halfList = pipePendings.get(dataPipeId);
									for (Jxta jxtaData : halfList) {
										socketFlow.addJxtaData(jxtaData);
									}
								}
								if(pipePendings.containsKey(ackPipeId)){
									ArrayList<Jxta> halfList = pipePendings.get(ackPipeId);
									for (Jxta jxtaAck : halfList) {
										socketFlow.addJxtaAck(jxtaAck);
									}
								}

								dataFlows.put(dataPipeId, socketFlow);
								ackFlows.put(ackPipeId, socketFlow);
							}
						}
					}	
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}

	}
}