package jxta.jnetpcap.countuptext;

import io.HdfsUtils;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Tcp;
import org.jnetpcap.protocol.tcpip.Udp;

public class CountUpTextMapper extends Mapper<NullWritable, Text, Text, Text> {

	public void map(NullWritable mapKey, Text value, Context context) throws IOException {		
		final Context ctx = context;		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path srcPath = new Path(value.toString());

		File pcapFile = HdfsUtils.getLocalFile(conf, hdfs, srcPath);
		if(pcapFile == null){
			pcapFile = HdfsUtils.getDistributedFile(value, hdfs, srcPath);
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
					Integer key = null;
					if(packet.hasHeader(Tcp.ID)){
						packet.getHeader(tcp);										
						key = tcp.destination();	
					}else 
						if(packet.hasHeader(Udp.ID)){
							packet.getHeader(udp);										
							key = udp.destination();
						}

					if(key != null){
						packet.getHeader(ip);
						Integer bc = ip.getLength() + ip.getPayloadLength();						
						try {
							ctx.write(new Text(key.toString()), new Text(bc + " " + 1));
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