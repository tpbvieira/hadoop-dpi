package org.apache.hadoop.hdfs.server.namenode;

public class NamenodeHack extends INodeFile{

	public void getFileINode(FSDirectory dir, String src) {
		System.out.println("### LocalName" + dir.getFileINode(src).getLocalName());
		System.out.println("### PathNames" + INode.getPathNames(src));
	}
	
	public void getFileInode(FSNamesystem ns, String src){
		System.out.println("### LocalName" + ns.dir.getFileINode(src).getLocalName());
		System.out.println("### PathNames" + INode.getPathNames(src));
	}
	
}