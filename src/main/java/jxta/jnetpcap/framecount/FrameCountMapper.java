package jxta.jnetpcap.framecount;

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

public class FrameCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static Text frameNum = new Text("frameNum");	

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {				

		Configuration conf = new Configuration();
System.out.println("### value:"+value);
		// Get file from HDFS and put on local file system
		FileSystem hdfs = FileSystem.get(conf);		
		Path srcPath = new Path(hdfs.getHomeDirectory() + "/" + value);
		Path dstPath = new Path("/tmp/");
		hdfs.copyToLocalFile(srcPath, dstPath);		

		System.out.println("#### Mapping " + "/tmp/" + value);
		final Context ctx = context;
		final StringBuilder errbuf = new StringBuilder();
		final Pcap pcap = Pcap.openOffline("/tmp/"+value, errbuf);		

		if (pcap == null) {
			throw new InterruptedException("Impossible create PCAP file");
		}

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
			IntWritable um = new IntWritable(1);
			public void nextPacket(JPacket packet, StringBuilder errbuf) {

				if(packet.getFrameNumber() > 0){
					try {
						ctx.write(frameNum, um);
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