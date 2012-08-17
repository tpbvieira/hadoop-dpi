package jxta.jnetpcap.countup;

import io.type.LongArrayWritable;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		Path dstPath = new Path("/tmp/");

		// Get File Name and Copy to local tmp dir
		StringTokenizer str = new StringTokenizer(value.toString(),"/");
		String fileName = null;
		while(str.hasMoreElements()){
			fileName = str.nextToken();
		}
		Path srcPath = new Path(value.toString());
		long t0 = System.currentTimeMillis();
		hdfs.copyToLocalFile(srcPath, dstPath);
		long t1 = System.currentTimeMillis();
		StringBuilder filePath = new StringBuilder(dstPath.toString());
		filePath.append("/");
		filePath.append(fileName);
		File pcapFile = new File(filePath.toString());	
		System.out.println("### CopyTime: " + (t1 - t0));
		
		
		
		// Teste		
//		System.out.println("### path: " + srcPath); 
//		System.out.println("### Exists: " + srcPath.getFileSystem(conf).getLocal(conf).exists(srcPath));
//		System.out.println("### Exists: " + FileSystem.getLocal(conf).exists(srcPath));
//		System.out.println("### CanonicalServiceName: " + srcPath.getFileSystem(conf).getCanonicalServiceName());		
//		System.out.println("### PathToFile: " + srcPath.getFileSystem(conf).getLocal(conf).pathToFile(srcPath));
//		FileStatus status = srcPath.getFileSystem(conf).getFileStatus(srcPath);
//		System.out.println("### Status: " + status);
//		BlockLocation[] blkLocations = srcPath.getFileSystem(conf).getFileBlockLocations(status, 0, Long.MAX_VALUE);
//		
//		System.out.println("### BlockLocations: " + blkLocations.length);
//		for (int i = 0; i < blkLocations.length; i++) {
//			String[] names = blkLocations[i].getNames();
//			for (int j = 0; j < names.length; j++) {
//				System.out.println("### Name: " + names[j]); 				
//			}
//		}
//		
//		System.out.println("### Block Locations ###");
//		DFSClient dfs = new DFSClient(DataNode.getDataNode().getNameNodeAddr(),conf);
//		LocatedBlocks blocks = dfs.namenode.getBlockLocations(value.toString(), 0, conf.getLong("dfs.block.size", FSConstants.DEFAULT_BLOCK_SIZE));
//		List<LocatedBlock> blockList = blocks.getLocatedBlocks();
//		for (LocatedBlock lBlock : blockList) {
//			System.out.println("### Block: " + lBlock.getBlock().getBlockName());
//		}
//
//		System.out.println("### HdfsFileStatus ###");	
//		HdfsFileStatus fileStatus = ((NameNode)dfs.namenode).getFileInfo(value.toString());
//		System.out.println("### BlockSize: " + fileStatus.getBlockSize());
//		System.out.println("### LocalName: " + fileStatus.getLocalName());
//
//		System.out.println("### FileINode ###");
//		NamenodeHack hack = new NamenodeHack();		
//		hack.getFileInode(((NameNode)dfs.namenode).namesystem, value.toString());

		
		
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