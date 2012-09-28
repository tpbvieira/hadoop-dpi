package io;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsHack;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.Text;

public class HdfsUtils {
	
	public static File getDistributedFile(Text value, FileSystem hdfs, Path srcPath)	throws IOException {
		File pcapFile;
		// Get File Name and Copy to local tmp dir
		System.out.println("### HDFS Access ###");
		StringTokenizer tokenizer = new StringTokenizer(value.toString(),"/");
		String fileName = null;
		while(tokenizer.hasMoreElements()){
			fileName = tokenizer.nextToken();
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
		return pcapFile;
	}

	public static File getLocalFile(Configuration conf, FileSystem hdfs, Path srcPath) {
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
						System.out.println("### Failed! PCAPFile NotFound");
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return pcapFile;
	}
}