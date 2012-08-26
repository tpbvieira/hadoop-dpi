package jxta.jnetpcap.countup;

import io.type.LongArrayWritable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsHack;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Tcp;
import org.jnetpcap.protocol.tcpip.Udp;

public class CountUpMapper extends Mapper<NullWritable, Text, IntWritable, LongArrayWritable> {

	public void map(NullWritable mapKey, Text value, Context context) throws IOException {		
		final Context ctx = context;		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path srcPath = new Path(value.toString());
		File pcapFile = null;

		try{
			System.out.println("### Local FS Access ###");
			String dataDir = null;
			String[] dataDirs = conf.getStrings("dfs.data.dir");
			if(dataDirs != null && dataDirs.length > 0){
				dataDir = dataDirs[0] + "/current/";
			}
			DistributedFileSystem dfs = (DistributedFileSystem)hdfs;
			DFSClient dfsClient = HdfsHack.getDFSCLient(dfs);
			LocatedBlocks blocks = dfsClient.namenode.getBlockLocations(HdfsHack.getPathName(dfs, srcPath), 0, conf.getLong("dfs.block.size", FSConstants.DEFAULT_BLOCK_SIZE));
			List<LocatedBlock> blockList = blocks.getLocatedBlocks();
			if(dataDir != null && blockList != null && blockList.size() > 0){				
				for (LocatedBlock locatedBlock : blockList) {
					String blockName = locatedBlock.getBlock().getBlockName();
					File tmpFile = new File(dataDir + blockName);
					if(tmpFile.exists()){
						pcapFile = tmpFile;
						System.out.println("### PCAPFile: " + pcapFile.getAbsoluteFile());
					}else{
						System.out.println("### PCAPFile NotFound");
//						FSDataInputStream in = hdfs.open(srcPath);
//						in.close();
//						if(tmpFile.exists()){
//							pcapFile = tmpFile;
//							System.out.println("### PCAPFile Received: " + pcapFile.getAbsoluteFile());
//						}else{
//							System.out.println("### PCAPFile NotFound Again!!!");
//						}
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		if(pcapFile == null){
			// Get File Name and Copy to local tmp dir
			System.out.println("### HDFS Access ###");
			StringTokenizer str = new StringTokenizer(value.toString(),"/");
			String fileName = null;
			while(str.hasMoreElements()){
				fileName = str.nextToken();
			}
			Path dstPath = new Path("/tmp/");
			long t0 = System.currentTimeMillis();
			hdfs.copyToLocalFile(srcPath, dstPath);
			long t1 = System.currentTimeMillis();
			StringBuilder filePath = new StringBuilder(dstPath.toString());
			filePath.append("/");
			filePath.append(fileName);
			pcapFile = new File(filePath.toString());	
			System.out.println("### CopyTime: " + (t1 - t0));
		}

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
					IntWritable key = null;
					if(packet.hasHeader(Tcp.ID)){
						packet.getHeader(tcp);										
						key = new IntWritable(tcp.destination());	
					}else 
						if(packet.hasHeader(Udp.ID)){
							packet.getHeader(udp);										
							key = new IntWritable(udp.destination());
						}

					if(key != null){
						packet.getHeader(ip);
						LongWritable[] values = new LongWritable[2];
						values[0] = new LongWritable(ip.getLength() + ip.getPayloadLength());
						values[1] = new LongWritable(1L);

						try {
							ctx.write(key, new LongArrayWritable(values));
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}	
					}
				}
			}
		}, errbuf);
		pcap.close();
	}
}