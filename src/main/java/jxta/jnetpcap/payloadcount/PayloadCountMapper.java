package jxta.jnetpcap.payloadcount;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.tcpip.Tcp;

public class PayloadCountMapper extends Mapper<NullWritable, Text, Text, IntWritable> {

	private final static Text wireLen = new Text("wireLen");
	private final static Text payloadLen = new Text("payloadLen");	

	protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {				
		final Context ctx = context;		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path dstPath = new Path("/tmp/");

		// Get File Name and Copy to local tmp dir
		StringTokenizer str = new StringTokenizer(value.toString(),"/");
		String fileName = null;
		while(str.hasMoreElements()){
			fileName = str.nextToken();
		}
		Path srcPath = new Path(value.toString());
		hdfs.copyToLocalFile(srcPath, dstPath);
		StringBuilder filePath = new StringBuilder(dstPath.toString());
		filePath.append("/");
		filePath.append(fileName);
		File pcapFile = new File(filePath.toString());	

		// Load file
		final StringBuilder errbuf = new StringBuilder();		
		final Pcap pcap = Pcap.openOffline(pcapFile.getAbsolutePath(), errbuf);
		if (pcap == null) {
			throw new RuntimeException("Impossible to open PCAP file");
		}

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
			final Tcp tcp = new Tcp();
			IntWritable num = new IntWritable(1);
			public void nextPacket(JPacket packet, StringBuilder errbuf) {
				if (packet.hasHeader(Tcp.ID)) {
					packet.getHeader(tcp);
					try {
						num.set(packet.getPacketWirelen());
						ctx.write(wireLen, num);
						num.set(tcp.getPayloadLength());
						ctx.write(payloadLen, num);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}, errbuf);
	}
}