package jxta.jnetpcap.framecount;

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

public class FrameCountMapper extends Mapper<NullWritable, Text, Text, IntWritable> {

	private final static Text frameNum = new Text("frameNum");
	private final static IntWritable um = new IntWritable(1);

	public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {				
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
			throw new InterruptedException("Impossible create PCAP file");
		}

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {			
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