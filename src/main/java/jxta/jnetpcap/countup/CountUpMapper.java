package jxta.jnetpcap.countup;

import io.type.LongArrayWritable;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Tcp;
import org.jnetpcap.protocol.tcpip.Udp;

@SuppressWarnings("deprecation")
public class CountUpMapper extends MapReduceBase implements Mapper<NullWritable, Text, Text, LongArrayWritable> {

	public static final Text jxtaRelyRttKey = new Text("rtt");
	public static final Text jxtaArrivalKey = new Text("arrival");
	public static final Text jxtaSocketReqKey = new Text("req");
	public static final Text jxtaSocketRemKey = new Text("rem");

	public void map(NullWritable mapKey, Text value, OutputCollector<Text, LongArrayWritable> output, Reporter reporter) throws IOException {
		final OutputCollector<Text, LongArrayWritable> context = output;
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path dstPath = new Path("/tmp/");

		// Get File Name
		System.out.println("\n### File: " + value);
		StringTokenizer str = new StringTokenizer(value.toString(),"/");
		String fileName = null;
		while(str.hasMoreElements()){
			fileName = str.nextToken();
		}

		// Copy to local tmp dir
		Path srcPath = new Path(value.toString());
		hdfs.copyToLocalFile(srcPath, dstPath);
		File pcapFile = new File(dstPath.toString() + "/" + fileName);	

		// Load file
		final StringBuilder errbuf = new StringBuilder();		
		final Pcap pcap = Pcap.openOffline(pcapFile.getAbsolutePath(), errbuf);
		if (pcap == null) {
			throw new RuntimeException("Impossible to open PCAP file");
		}

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
			Ip4 ip = new Ip4();
			Tcp tcp = new Tcp();
			Udp udp = new Udp();

			public void nextPacket(JPacket packet, StringBuilder errbuf) {
				if(packet.hasHeader(Ip4.ID)){
					
					Text keyValue = null;
					
					if(packet.hasHeader(Tcp.ID)){
						packet.getHeader(tcp);										
						keyValue = new Text(Integer.toString(tcp.destination()));	
					}else 
						if(packet.hasHeader(Udp.ID)){
							packet.getHeader(udp);										
							keyValue = new Text(Integer.toString(udp.destination()));
						}
					
					if(keyValue != null){
						packet.getHeader(ip);
						LongWritable[] values = new LongWritable[2];
						values[0] = new LongWritable(ip.getLength() + ip.getPayloadLength());
						values[1] = new LongWritable(1L);
						
						
						try {
							context.collect(keyValue, new LongArrayWritable(values));
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
				}
			}

		}, errbuf);

		pcap.close();
	}
}