package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;

public class HdfsHack {
	
	public static DFSClient getDFSCLient(DistributedFileSystem dfs){
		return dfs.dfs;
	}

	public static LocatedBlocks callGetBlockLocations(ClientProtocol namenode, String src, long start, long length) throws IOException {
		try {
			return namenode.getBlockLocations(src, start, length);
		} catch(RemoteException re) {
			throw re.unwrapRemoteException(AccessControlException.class,FileNotFoundException.class);
		}
	}

	public static String getPathName(DistributedFileSystem dfs, Path file) {
		String result = makeAbsolute(dfs, file).toUri().getPath();
		if (!DFSUtil.isValidName(result)) {
			throw new IllegalArgumentException("Pathname " + result + " from " + file+" is not a valid DFS filename.");
		}
		return result;
	}

	private static Path makeAbsolute(DistributedFileSystem dfs, Path f) {
		if (f.isAbsolute()) {
			return f;
		} else {
			return new Path(dfs.getWorkingDirectory(), f);
		}
	}
}
