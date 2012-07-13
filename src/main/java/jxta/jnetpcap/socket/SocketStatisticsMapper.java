package jxta.jnetpcap.socket;

import io.type.LongArrayWritable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import net.jxta.document.AdvertisementFactory;
import net.jxta.document.StructuredDocumentFactory;
import net.jxta.document.XMLDocument;
import net.jxta.endpoint.Message;
import net.jxta.endpoint.Message.ElementIterator;
import net.jxta.endpoint.MessageElement;
import net.jxta.impl.endpoint.router.EndpointRouterMessage;
import net.jxta.impl.util.pipe.reliable.Defs;
import net.jxta.protocol.PipeAdvertisement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Jxta;
import org.jnetpcap.protocol.tcpip.Jxta.JxtaMessageType;
import org.jnetpcap.protocol.tcpip.JxtaUtils;
import org.jnetpcap.protocol.tcpip.Tcp;

@SuppressWarnings("deprecation")
public class SocketStatisticsMapper extends MapReduceBase implements Mapper<NullWritable, Text, Text, SortedMapWritable> {

	public static final Text jxtaRelyRttKey = new Text("rtt");
	public static final Text jxtaArrivalKey = new Text("arrival");
	public static final Text jxtaSocketReqKey = new Text("req");
	public static final Text jxtaSocketRemKey = new Text("rem");

	private static final String socketNamespace = "JXTASOC";
	private static final String reqPipeTag = "reqPipe";
	private static final String remPipeTag = "remPipe";	
	private static final String closeTag = "close";

	public void map(NullWritable mapKey, Text value, OutputCollector<Text, SortedMapWritable> output, Reporter reporter) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path dstPath = new Path("/tmp/");
		double t0=0,t1=0,t2=0,t3=0;

		// Get File Name
		System.out.println("\n\n### File: " + value);
		StringTokenizer str = new StringTokenizer(value.toString(),"/");
		String fileName = null;
		while(str.hasMoreElements()){
			fileName = str.nextToken();
		}

		t0 = System.currentTimeMillis();
		Path srcPath = new Path(value.toString());
		hdfs.copyToLocalFile(srcPath, dstPath);
		File pcapFile = new File(dstPath.toString() + "/" + fileName);
		t1 = System.currentTimeMillis();	

		final StringBuilder errbuf = new StringBuilder();		
		final Pcap pcap = Pcap.openOffline(pcapFile.getAbsolutePath(), errbuf);
		if (pcap == null) {
			throw new RuntimeException("Impossible to open PCAP file");
		}

		final SortedMap<Integer,JxtaSocketFlow> dataFlows = new TreeMap<Integer,JxtaSocketFlow>();
		final SortedMap<Integer,JxtaSocketFlow> ackFlows = new TreeMap<Integer,JxtaSocketFlow>();

		t2 = System.currentTimeMillis();
		generateHalfSocketFlows(errbuf, pcap, dataFlows, ackFlows);
		t3 = System.currentTimeMillis();

		System.out.println("### CopyTime: " + (t1-t0));
		System.out.println("### FlowTime: " + (t3-t2));
		System.out.println("### T1: " + (t1-t0)/(t3-t2));
		System.out.println("### T2: " + (t1-t0)/((t1-t0) + (t3-t2)));

		generateJxtaRttStatistics(output,dataFlows,ackFlows);

		pcap.close();		
		System.out.println("### " + fileName + " ? deleted == " + pcapFile.delete());
	}

	private void generateHalfSocketFlows(final StringBuilder errbuf, final Pcap pcap, final SortedMap<Integer,JxtaSocketFlow> dataFlows,
			final SortedMap<Integer,JxtaSocketFlow> ackFlows) {

		final SortedMap<Integer,Jxta> fragments = new TreeMap<Integer,Jxta>();		
		final SortedMap<Integer,Jxta> reqPendings = new TreeMap<Integer,Jxta>();
		final SortedMap<Integer,ArrayList<Jxta>> pipePendings = new TreeMap<Integer,ArrayList<Jxta>>();

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
			Tcp tcp = new Tcp();
			Jxta jxta = new Jxta();

			public void nextPacket(JPacket packet, StringBuilder errbuf) {

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
								updateHalfSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
								fragments.remove(tcpFlowId);
								if(byteBuffer.hasRemaining()){
									try{
										jxta = new Jxta();
										packet.getHeader(jxta);
										byte[] rest = new byte[byteBuffer.remaining()];
										byteBuffer.get(rest, 0, byteBuffer.remaining());
										byteBuffer = ByteBuffer.wrap(rest);
										jxta.decode(seqNumber,packet,byteBuffer);										
										updateHalfSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
										if(byteBuffer.hasRemaining()){											
											jxta = new Jxta();
											packet.getHeader(jxta);
											byte[] rest2 = new byte[byteBuffer.remaining()];
											byteBuffer.get(rest2, 0, byteBuffer.remaining());
											jxta.decode(seqNumber,packet,ByteBuffer.wrap(rest2));										
											updateHalfSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
											System.out.println("### Return 0!");
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
									}catch (RuntimeException e) {
										if(e.getMessage().equals("Error on header parser")){
											SortedMap<Long,JPacket> packets = jxta.getJxtaPackets();
											packets.clear();
											packets.put(seqNumber,packet);
											fragments.put(tcpFlowId,jxta);	
										}else{
											e.printStackTrace();	
										}
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
								updateHalfSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
								if(jxta.isFragmented()){									
									ByteBuffer remain = ByteBuffer.wrap(jxta.getRemain());									
									jxta = new Jxta();
									packet.getHeader(jxta);
									jxta.decode(remain);
									jxta.getJxtaPackets().put(seqNumber,packet);
									updateHalfSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
									if(jxta.isFragmented()){									
										remain = ByteBuffer.wrap(jxta.getRemain());									
										jxta = new Jxta();
										packet.getHeader(jxta);
										jxta.decode(remain);
										updateHalfSocketFlows(jxta,reqPendings,pipePendings,dataFlows,ackFlows);
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

		if(fragments.size() > 0)
			System.out.println("### Fragments: " + fragments.size());

		if(reqPendings.size() > 0)
			System.out.println("### ReqPipe Pendings: " + reqPendings.size());

		if(pipePendings.size() > 0)
			System.out.println("### PipePendings: " + pipePendings.size());
	}

	@SuppressWarnings("rawtypes")
	public void updateHalfSocketFlows(Jxta jxta, SortedMap<Integer,Jxta> reqPendings, SortedMap<Integer,ArrayList<Jxta>> pipePendings, SortedMap<Integer,JxtaSocketFlow> dataFlows, SortedMap<Integer,JxtaSocketFlow> ackFlows){
		MessageElement el = null;
		ElementIterator elements;
		Message msg = jxta.getMessage();

		try{
			elements = msg.getMessageElements(Defs.NAMESPACE, Defs.MIME_TYPE_BLOCK);// jxta-reliable-block
			if(elements.hasNext()){
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

	@SuppressWarnings("unused")
	private void generateJxtaStatistics(OutputCollector<Text, SortedMapWritable> ctx, SortedMap<Integer,JxtaSocketFlow> dataFlows, SortedMap<Integer,JxtaSocketFlow> ackFlows){
		//		final Text jxtaPayloadKey = new Text("JXTA_Payload");
		//		final Text jxtaTimeKey = new Text("JXTA_Time");
		//		final Text jxtaThroughputKey = new Text("JXTA_Throughput");
		final Text jxtaRelyRttKey = new Text("JXTA_Reliability_RTT");
		final Text jxtaArrivalKey = new Text("JXTA_Rely_Arrival");

		//		final SortedMapWritable payOutput = new SortedMapWritable();
		//		final SortedMapWritable timeOutput = new SortedMapWritable();
		//		final SortedMapWritable throughput = new SortedMapWritable();		
		final SortedMapWritable rttOutput = new SortedMapWritable();
		final SortedMapWritable arrivalOutput = new SortedMapWritable();
		SortedMap<Integer,Long[]> rtts;
		int uncompleted = 0;
		int ackLost = 0;

		for (Integer dataFlowKey : dataFlows.keySet()) {			
			final JxtaSocketFlow dataFlow = dataFlows.get(dataFlowKey);

			//			payOutput.put(new LongWritable(dataFlow.getEndTime()), new DoubleWritable((dataFlow.getPayload())/1024));			
			//			timeOutput.put(new LongWritable(dataFlowKey), new DoubleWritable(dataFlow.getTransferTime()));
			//			throughput.put(new LongWritable(dataFlowKey), new DoubleWritable((dataFlow.getPayload()/1024)/(dataFlow.getTransferTime()/1000)));
			if(dataFlow.isAckComplete() && dataFlow.isDataComplete()){				
				rtts = dataFlow.getRtts();
				for (Integer num : rtts.keySet()) {
					LongWritable key = new LongWritable(dataFlow.getEndTime() + num);
					Long[] rtt = rtts.get(num);
					rttOutput.put(key, new DoubleWritable(rtt[1] - rtt[0]));

					LongWritable arrivalTime = new LongWritable(rtt[0]);
					if(arrivalOutput.containsKey(arrivalTime)){
						LongWritable arrivalCount = (LongWritable)arrivalOutput.get(arrivalTime);
						arrivalCount.set(arrivalCount.get() + 1);
					}else{
						arrivalOutput.put(arrivalTime, new LongWritable(1));	
					}
				}
			}else{
				uncompleted++;
				//				System.out.println(dataFlow);
				rtts = dataFlow.getRtts();
				for (Integer num : rtts.keySet()) {
					LongWritable key = new LongWritable(dataFlow.getEndTime() + num);
					Long[] rtt = rtts.get(num);
					if(rtt != null && rtt[0] != null && rtt[1] != null){
						rttOutput.put(key, new DoubleWritable(rtt[1] - rtt[0]));

						LongWritable arrivalTime = new LongWritable(rtt[0]);
						if(arrivalOutput.containsKey(arrivalTime)){
							LongWritable arrivalCount = (LongWritable)arrivalOutput.get(arrivalTime);
							arrivalCount.set(arrivalCount.get() + 1);
						}else{
							arrivalOutput.put(arrivalTime, new LongWritable(1));	
						}

					}else
						if(rtt != null && rtt[0] != null && rtt[1] == null){
							LongWritable arrivalTime = new LongWritable(rtt[0]);
							if(arrivalOutput.containsKey(arrivalTime)){
								LongWritable arrivalCount = (LongWritable)arrivalOutput.get(arrivalTime);
								arrivalCount.set(arrivalCount.get() + 1);
							}else{
								arrivalOutput.put(arrivalTime, new LongWritable(1));	
							}
							ackLost++;
						}
				}
			}
		}

		if(uncompleted > 0)
			System.out.println("### Total Uncompleted Flows = " + uncompleted);
		if(ackLost > 0)
			System.out.println("### Ack Expected = " + ackLost);

		try{
			//			ctx.collect(jxtaPayloadKey, payOutput);
			//			ctx.collect(jxtaTimeKey, timeOutput);
			//			ctx.collect(jxtaThroughputKey, throughput);
			ctx.collect(jxtaRelyRttKey, rttOutput);
			System.out.println("### Size: " + rttOutput.size());

			ctx.collect(jxtaArrivalKey, arrivalOutput);
			System.out.println("### Arrivals: " + arrivalOutput.size());

		}catch(IOException e){
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void generateJxtaRttStatistics(OutputCollector<Text, SortedMapWritable> ctx, SortedMap<Integer,JxtaSocketFlow> dataFlows, SortedMap<Integer,JxtaSocketFlow> ackFlows){

		final SortedMapWritable socReqOutput = new SortedMapWritable();
		final SortedMapWritable socRemOutput = new SortedMapWritable();
		final SortedMapWritable arrivalOutput = new SortedMapWritable();

		HashMap<Long,ArrayList<LongWritable>> timeRtt = new HashMap<Long,ArrayList<LongWritable>>();
		SortedMap<Integer,Long[]> rtts;		

		int uncompleted = 0;
		int ackLost = 0;

		for (Integer dataFlowKey : dataFlows.keySet()) {

			final JxtaSocketFlow dataFlow = dataFlows.get(dataFlowKey);

			// Socket Request Time
			if(dataFlow.getSocketReqTime() > 0){
				LongWritable reqTime = new LongWritable(dataFlow.getSocketReqTime()/1000);
				if(socReqOutput.containsKey(reqTime)){
					LongWritable reqCount = (LongWritable)socReqOutput.get(reqTime);
					reqCount.set(reqCount.get() + 1);
				}else{
					socReqOutput.put(reqTime, new LongWritable(1));	
				}	
			}

			// Socket Response Time
			if(dataFlow.getSocketRemTime() > 0){
				LongWritable remTime = new LongWritable(dataFlow.getSocketRemTime()/1000);
				if(socRemOutput.containsKey(remTime)){
					LongWritable remCount = (LongWritable)socRemOutput.get(remTime);
					remCount.set(remCount.get() + 1);
				}else{
					socRemOutput.put(remTime, new LongWritable(1));	
				}	
			}

			if(dataFlow.isAckComplete() && dataFlow.isDataComplete()){				
				rtts = dataFlow.getRtts();
				for (Integer num : rtts.keySet()) {
					// RTT
					Long[] rtt = rtts.get(num);
					if(timeRtt.containsKey(rtt[1]/1000)){
						timeRtt.get(rtt[1]/1000).add(new LongWritable(rtt[1] - rtt[0]));
					}else{
						ArrayList<LongWritable> lst = new ArrayList<LongWritable>();
						lst.add(new LongWritable(rtt[1] - rtt[0]));
						timeRtt.put(rtt[1]/1000, lst);
					}

					// Arrival count
					LongWritable arrivalTime = new LongWritable(rtt[0]/1000);
					if(arrivalOutput.containsKey(arrivalTime)){
						LongWritable arrivalCount = (LongWritable)arrivalOutput.get(arrivalTime);
						arrivalCount.set(arrivalCount.get() + 1);
					}else{
						arrivalOutput.put(arrivalTime, new LongWritable(1));	
					}
				}
			}else{
				uncompleted++;

				rtts = dataFlow.getRtts();
				for (Integer num : rtts.keySet()) {

					Long[] rtt = rtts.get(num);					
					if(rtt != null && rtt[0] != null && rtt[1] != null){
						// RTT
						if(timeRtt.containsKey(rtt[1]/1000)){
							timeRtt.get(rtt[1]/1000).add(new LongWritable(rtt[1] - rtt[0]));
						}else{
							ArrayList<LongWritable> lst = new ArrayList<LongWritable>();
							lst.add(new LongWritable(rtt[1] - rtt[0]));
							timeRtt.put(rtt[1]/1000, lst);
						}

						// Arrival Count
						LongWritable arrival = new LongWritable(rtt[0]/1000);
						if(arrivalOutput.containsKey(arrival)){
							LongWritable arrivalCount = (LongWritable)arrivalOutput.get(arrival);
							arrivalCount.set(arrivalCount.get() + 1);
						}else{
							arrivalOutput.put(arrival, new LongWritable(1));	
						}

					}else
						if(rtt != null && rtt[0] != null && rtt[1] == null){

							// Arrival Count
							LongWritable arrival = new LongWritable(rtt[0]/1000);
							if(arrivalOutput.containsKey(arrival)){
								LongWritable arrivalCount = (LongWritable)arrivalOutput.get(arrival);
								arrivalCount.set(arrivalCount.get() + 1);
							}else{
								arrivalOutput.put(arrival, new LongWritable(1));	
							}
							ackLost++;
						}
				}
			}
		}

		if(uncompleted > 0)
			System.out.println("### Total Uncompleted Flows = " + uncompleted);
		if(ackLost > 0)
			System.out.println("### Ack Expected = " + ackLost);

		try{
			// Socket Request
			ctx.collect(jxtaSocketReqKey, socReqOutput);
			System.out.println("### Socket Requests: " + socReqOutput.size());

			// Socket Response
			ctx.collect(jxtaSocketRemKey, socRemOutput);
			System.out.println("### Socket Response: " + socRemOutput.size());

			// Arrivals
			ctx.collect(jxtaArrivalKey, arrivalOutput);
			System.out.println("### Arrivals: " + arrivalOutput.size());

			// RTT
			final SortedMapWritable rttOutput = new SortedMapWritable();
			Set<Long> times = timeRtt.keySet();			
			for (Long time : times) {
				ArrayList<LongWritable> rttPerTime = timeRtt.get(time);
				LongWritable[] fd = new LongWritable[rttPerTime.size()];
				int i = 0;
				for (LongWritable longWritable : rttPerTime) {
					fd[i] = new LongWritable(longWritable.get());
					i++;
				}
				rttOutput.put(new LongWritable(time), new LongArrayWritable(fd));				
			}
			ctx.collect(jxtaRelyRttKey, rttOutput);
			System.out.println("### Rtts: " + rttOutput.size());			
		}catch(IOException e){
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}