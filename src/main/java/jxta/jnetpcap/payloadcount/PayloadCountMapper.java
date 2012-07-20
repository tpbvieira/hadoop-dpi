package jxta.jnetpcap.payloadcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.tcpip.Tcp;

public class PayloadCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static Text wireLen = new Text("wireLen");
	private final static Text payloadLen = new Text("payloadLen");	

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {				
		System.out.println("### Value:" + value);
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);		
		Path srcPath = new Path(hdfs.getWorkingDirectory() + "/" + value);
		Path dstPath = new Path("/tmp/");
		System.out.println("### hdfsSrc:" + srcPath);
		System.out.println("### localDst:" + dstPath);
		hdfs.copyToLocalFile(srcPath, dstPath);
		
		final Context ctx = context;
		final StringBuilder errbuf = new StringBuilder();
		final Pcap pcap = Pcap.openOffline("/tmp/" + value, errbuf);		

		if (pcap == null) {
			throw new InterruptedException("Impossible create PCAP file");
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