package jxta.jnetpcap.countup;

import io.type.LongArrayWritable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Tcp;
import org.jnetpcap.protocol.tcpip.Udp;

import util.FileUtil;

public class CountUp {

	public static final Text jxtaRelyRttKey = new Text("rtt");
	public static final Text jxtaArrivalKey = new Text("arrival");
	public static final Text jxtaSocketReqKey = new Text("req");
	public static final Text jxtaSocketRemKey = new Text("rem");

	private static int executions = 10;
	private static String inputDir = "/home/thiago/tmp/pcap-traces/dropbox/3.5/32/";	

	/**
	 * @param args
	 * @throws IOException 
	 */	
	public static void main(String[] args) throws IOException {
		
		long t0,t1;

		if(args != null && args.length == 2){
			executions = Integer.valueOf(args[0]);
			inputDir = args[1];
		}

		for (int i = 0; i < executions; i++) {
			t0 = System.currentTimeMillis();
			execute();
			t1 = System.currentTimeMillis();
			System.out.println("### Time: " + (t1 - t0));
		}
	}

	/**
	 * 
	 * @throws IOException 
	 */	
	private static void execute() throws IOException {
		Pcap pcap;
		final StringBuilder errbuf = new StringBuilder();
		Map<Text,List<LongArrayWritable>> values = new HashMap<Text,List<LongArrayWritable>>();
		File path = new File(inputDir);

		if(path.isDirectory()){
			File[] files = FileUtil.listFiles(path);
			for (int i = 0; i < files.length; i++) {
				pcap = Pcap.openOffline(files[i].getAbsolutePath(), errbuf);
				if (pcap == null) {
					throw new RuntimeException("Impossible to open PCAP file");
				}

				extractValues(pcap,values);				
				pcap.close();
			}
		}else{
			pcap = Pcap.openOffline(path.getAbsolutePath(), errbuf);
			if (pcap == null) {
				throw new RuntimeException("Impossible to open PCAP file");
			}

			extractValues(pcap,values);
			pcap.close();
		}

		StringBuilder strOutput = new StringBuilder();
		long bc = 0;
		long pc = 0;
		Set<Text> keys = values.keySet();			
		for (Text key : keys) {
			List<LongArrayWritable> lst = values.get(key);
			bc = 0;
			pc = 0;
			for (LongArrayWritable arr : lst) {	
				Writable[] entry = arr.get();
				bc += Long.parseLong(((LongWritable)entry[0]).toString());
				pc += Long.parseLong(((LongWritable)entry[1]).toString());
			}			
			strOutput.append(key);
			strOutput.append(" ");
			strOutput.append(bc);
			strOutput.append(" ");
			strOutput.append(pc);
			strOutput.append("\n");
		}
		FileUtils.writeStringToFile(new File("output.txt"), strOutput.toString());
	}

	public static void extractValues(Pcap pcap, final Map<Text,List<LongArrayWritable>> values){

		final StringBuilder errbuf = new StringBuilder();

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
			Ip4 ip = new Ip4();
			Tcp tcp = new Tcp();
			Udp udp = new Udp();

			public void nextPacket(JPacket packet, StringBuilder errbuf) {
				if(packet.hasHeader(Ip4.ID)){

					Text key = null;

					if(packet.hasHeader(Tcp.ID)){
						packet.getHeader(tcp);										
						key = new Text(Integer.toString(tcp.destination()));	
					}else 
						if(packet.hasHeader(Udp.ID)){
							packet.getHeader(udp);										
							key = new Text(Integer.toString(udp.destination()));
						}

					if(key != null){
						packet.getHeader(ip);
						LongWritable[] tuple = new LongWritable[2];
						tuple[0] = new LongWritable(ip.getLength() + ip.getPayloadLength());
						tuple[1] = new LongWritable(1L);

						List<LongArrayWritable> lst = values.get(key);
						if(lst != null){
							lst.add(new LongArrayWritable(tuple));
						}else{
							lst = new ArrayList<LongArrayWritable>();							
							lst.add(new LongArrayWritable(tuple));
							values.put(key, lst);
						}
					}
				}
			}

		}, errbuf);

	}
}