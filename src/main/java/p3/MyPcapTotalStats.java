package p3;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Tcp;

import util.FileUtil;

public class MyPcapTotalStats {

	public static final Text jxtaRelyRttKey = new Text("rtt");
	public static final Text jxtaArrivalKey = new Text("arrival");
	public static final Text jxtaSocketReqKey = new Text("req");
	public static final Text jxtaSocketRemKey = new Text("rem");

	private static int executions = 1;
	private static String inputDir = "/home/thiago/tmp/_/";	

	/**
	 * @param args
	 * @throws IOException 
	 */	
	public static void main(String[] args) throws IOException {

		if(args != null && args.length == 2){
			executions = Integer.valueOf(args[0]);
			inputDir = args[1];
		}

		for (int i = 0; i < executions; i++) {
			execute();
		}
	}

	/**
	 * 
	 * @throws IOException 
	 */	
	private static void execute() throws IOException {
		Pcap pcap;
		StringBuilder errbuf = new StringBuilder();
		File path = new File(inputDir);

		if(path.isDirectory()){
			File[] files = FileUtil.listFiles(path);
			for (int i = 0; i < files.length; i++) {
				pcap = Pcap.openOffline(files[i].getAbsolutePath(), errbuf);
				if (pcap == null) {
					throw new RuntimeException("Impossible to open PCAP file");
				}
				generateSocketFlows(errbuf, pcap);
				pcap.close();
			}
		}else{
			pcap = Pcap.openOffline(path.getAbsolutePath(), errbuf);
			if (pcap == null) {
				throw new RuntimeException("Impossible to open PCAP file");
			}
			generateSocketFlows(errbuf, pcap);
			pcap.close();
		}
	}

	public static void generateSocketFlows(final StringBuilder errbuf, final Pcap pcap) {

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
			Ip4 ip = new Ip4();
			Tcp tcp = new Tcp();
			
			long len = 0;
			long pkts = 0;

			public void nextPacket(JPacket packet, StringBuilder errbuf) {
				if(packet.hasHeader(Ip4.ID)){
					packet.getHeader(ip);

					len += ip.length();
					pkts++;
					System.out.println("### " + len + " " + pkts);
					
					// IPv4 bc	11258050					
					// IPv4 pc	9560
					
					
					
					// IPv4 addr	2

					// IPv4 dstaddr	2
					// IPv4 flows	12

					// IPv4 srcaddr	2

					if(packet.hasHeader(Tcp.ID)){
						packet.getHeader(tcp);
						// IPv4 tcp dstPort	3
						// IPv4 tcp srcPort	3
					}


				}
			}

			
		}, errbuf);

//		long sum = 0;
//		int new_value = 1;         	
//
//		String[] tok = key.toString().split(":");          
//		if(tok.length > 1){
//			output.collect(key, new LongWritable(new_value));        	   
//		}else{
//			while(value.hasNext()) 		 				
//				sum += value.next().get();
//			output.collect(key, new LongWritable(sum));        
//		}
	}
}