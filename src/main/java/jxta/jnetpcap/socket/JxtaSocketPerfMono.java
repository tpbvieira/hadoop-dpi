package jxta.jnetpcap.socket;

import io.type.LongArrayWritable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.jnetpcap.Pcap;

import util.FileUtil;

public class JxtaSocketPerfMono {

	public static final Text jxtaRelyRttKey = new Text("rtt");
	public static final Text jxtaArrivalKey = new Text("arv");
	public static final Text jxtaSocketReqKey = new Text("req");
	public static final Text jxtaSocketRemKey = new Text("rem");

	private static boolean isProgressive = false;
	private static int executions = 1;
	private static int limit = 1;
	private static String inputDir = "/home/thiago/tmp/pcap-traces/jxtaSocket/3.5/128/";	
	private static String outputDir = "output/JxtaSocketPerfMono_" + System.currentTimeMillis() + "/";

	public static void main(String[] args) throws IOException {		

		if(args != null && args.length >= 2){
			executions = Integer.valueOf(args[0]);
			inputDir = args[1];
			isProgressive = (args.length > 2);
		}
		
		if(isProgressive){
			limit = Integer.valueOf(args[2]);
			for (int i = 1; i <= limit; i++) {
				System.out.println("\n### LimitInfo: " + i + " of " + limit);
				System.out.println("### Executions: " + executions);
				for (int j = 0; j < executions; j++) {
					execute(i);
				}	
			}
		}else{
			limit = Integer.MAX_VALUE;
			for (int j = 0; j < executions; j++) {
				execute(limit);
			}
		}
	}

	/**
	 * 
	 * @throws IOException 
	 */	
	@SuppressWarnings("rawtypes")
	private static void execute(int limit) throws IOException {
		final StringBuilder errbuf = new StringBuilder();
		final SortedMap<Integer,JxtaSocketFlow> dataFlows = new TreeMap<Integer,JxtaSocketFlow>();
		final SortedMap<Integer,JxtaSocketFlow> ackFlows = new TreeMap<Integer,JxtaSocketFlow>();
		File path = new File(inputDir);
		Pcap pcap;

		long t0 = System.currentTimeMillis();
		if(path.isDirectory()){
			File[] files = FileUtil.listFiles(path);
			for (int i = 0; i < files.length && i < limit; i++) {
				System.out.println("### File: " + files[i].getAbsolutePath());
				pcap = Pcap.openOffline(files[i].getAbsolutePath(), errbuf);
				if (pcap == null) {
					throw new RuntimeException("Impossible to open PCAP file");
				}

				JxtaSocketFlow.generateSocketFlows(errbuf, pcap, dataFlows, ackFlows);
				pcap.close();
			}
		}else{
			pcap = Pcap.openOffline(path.getAbsolutePath(), errbuf);
			if (pcap == null) {
				throw new RuntimeException("Impossible to open PCAP file");
			}

			JxtaSocketFlow.generateSocketFlows(errbuf, pcap, dataFlows, ackFlows);
			pcap.close();
		}

		Map <Text,SortedMapWritable> output = generateJxtaRttStatistics(dataFlows,ackFlows);

		StringBuilder strOutput = new StringBuilder();
		SortedMapWritable tmp = output.get(jxtaArrivalKey);
		Set<WritableComparable> arrivals = tmp.keySet();
		for (WritableComparable arrivalTime : arrivals) {
			strOutput.append("\n");
			strOutput.append((LongWritable)arrivalTime);
			strOutput.append(" ");
			strOutput.append(((LongWritable)tmp.get(arrivalTime)).get());		
		}
		FileUtils.writeStringToFile(new File(outputDir + jxtaArrivalKey.toString()), strOutput.toString());

		strOutput = new StringBuilder();
		tmp = output.get(jxtaRelyRttKey);		
		Set<WritableComparable> times = tmp.keySet();
		for (WritableComparable time : times) {
			ArrayWritable rttsArray = (ArrayWritable)tmp.get(time);
			Writable[] rtts = rttsArray.get();						
			for (int j = 0; j < rtts.length; j++) {
				strOutput.append("\n");
				strOutput.append(time);
				strOutput.append(" ");
				strOutput.append(((LongWritable)rtts[j]).get());
			}
		}
		FileUtils.writeStringToFile(new File(outputDir + jxtaRelyRttKey.toString()), strOutput.toString());

		strOutput = new StringBuilder();
		tmp = output.get(jxtaSocketReqKey);
		Set<WritableComparable> requests = tmp.keySet();
		for (WritableComparable requestTime : requests) {
			strOutput.append("\n");
			strOutput.append((LongWritable)requestTime);
			strOutput.append(" ");
			strOutput.append(((LongWritable)tmp.get(requestTime)).get());		
		}
		FileUtils.writeStringToFile(new File(outputDir + jxtaSocketReqKey.toString()), strOutput.toString());

		strOutput = new StringBuilder();
		tmp = output.get(jxtaSocketRemKey);						
		Set<WritableComparable> responses = tmp.keySet();
		for (WritableComparable responseTime : responses) {
			strOutput.append("\n");
			strOutput.append((LongWritable)responseTime);
			strOutput.append(" ");
			strOutput.append(((LongWritable)tmp.get(responseTime)).get());		
		}
		FileUtils.writeStringToFile(new File(outputDir + jxtaSocketRemKey.toString()), strOutput.toString());

		long t1 = System.currentTimeMillis();

		System.out.println("### Execution Time: " + (t1 - t0));
	}

	public static Map<Text,SortedMapWritable> generateJxtaRttStatistics(SortedMap<Integer,JxtaSocketFlow> dataFlows, SortedMap<Integer,JxtaSocketFlow> ackFlows){

		HashMap <Text,SortedMapWritable> output = new HashMap<Text,SortedMapWritable>();

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

		if(uncompleted > 0 && JxtaSocketFlow.isDebug)
			System.out.println("### Total Uncompleted Flows = " + uncompleted);
		if(ackLost >  0 && JxtaSocketFlow.isDebug)
			System.out.println("### Ack Expected = " + ackLost);

		// Socket Request
		output.put(jxtaSocketReqKey, socReqOutput);
		if(JxtaSocketFlow.isDebug)
			System.out.println("### Socket Requests: " + socReqOutput.size());

		// Socket Response
		output.put(jxtaSocketRemKey, socRemOutput);
		if(JxtaSocketFlow.isDebug)
			System.out.println("### Socket Response: " + socRemOutput.size());

		// Arrivals
		output.put(jxtaArrivalKey, arrivalOutput);
		if(JxtaSocketFlow.isDebug)
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
		output.put(jxtaRelyRttKey, rttOutput);
		if(JxtaSocketFlow.isDebug)
			System.out.println("### Rtts: " + rttOutput.size());			

		return output;
	}
}