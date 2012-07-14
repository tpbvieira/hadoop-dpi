package jxta.jnetpcap.socket;

import io.type.LongArrayWritable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jnetpcap.Pcap;

@SuppressWarnings("deprecation")
public class JxtaSocketPerfMapper extends MapReduceBase implements Mapper<NullWritable, Text, Text, SortedMapWritable> {

	public static final Text jxtaRelyRttKey = new Text("rtt");
	public static final Text jxtaArrivalKey = new Text("arrival");
	public static final Text jxtaSocketReqKey = new Text("req");
	public static final Text jxtaSocketRemKey = new Text("rem");

	public void map(NullWritable mapKey, Text value, OutputCollector<Text, SortedMapWritable> output, Reporter reporter) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path dstPath = new Path("/tmp/");
		double t0=0,t1=0,t2=0,t3=0;

		// Get File Name
		System.out.println("\n### File: " + value);
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
		JxtaSocketFlow.generateSocketFlows(errbuf, pcap, dataFlows, ackFlows);
		t3 = System.currentTimeMillis();

		System.out.println("### CopyTime: " + (t1-t0));
		System.out.println("### FlowTime: " + (t3-t2));
		System.out.println("### CopyTime/FlowTime: " + (t1-t0)/(t3-t2));
		System.out.println("### CopyTime/TotalTime: " + (t1-t0)/((t1-t0) + (t3-t2)));

		generateJxtaStatistics(output,dataFlows,ackFlows);

		pcap.close();
	}	

	private void generateJxtaStatistics(OutputCollector<Text, SortedMapWritable> ctx, SortedMap<Integer,JxtaSocketFlow> dataFlows, SortedMap<Integer,JxtaSocketFlow> ackFlows){

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