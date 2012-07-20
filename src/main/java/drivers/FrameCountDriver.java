package drivers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import jxta.jnetpcap.framecount.FrameCountMapper;
import jxta.jnetpcap.framecount.FrameCountReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FrameCountDriver {

	private static final String localInputPcap = "/home/thiago/tmp/pcap-traces/_/";
	private static final String localInput = "input/";
	private static final String localOutput = "output/";
	private static final String inputFile = "fileList.txt";
	
	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "jnetpcap.payloadcount");

		// Generate input with file's list
		File inputList = new File(localInputPcap);
		String[] fileList = inputList.list();
		PrintWriter file = new PrintWriter(new BufferedWriter(new FileWriter(localInput + inputFile)));
		for (int i = 0; i < fileList.length; i++) {
			file.println(fileList[i]);
		}
		file.close();		
		
		// Copy local files into HDFS /
		FileSystem hdfs = FileSystem.get(conf);
		Path dfsFilesPath = new Path(hdfs.getHomeDirectory().toString());
		Path dsfInputPath = new Path(hdfs.getHomeDirectory() + "/input");		
		hdfs.mkdirs(dsfInputPath);		
		for (int i = 0; i < fileList.length; i++) {
			Path localFile = new Path(localInputPcap + fileList[i]);		
			hdfs.copyFromLocalFile(localFile, dfsFilesPath);
			System.out.println("### from:" + localFile + " to:" + dfsFilesPath );
		}		
		hdfs.copyFromLocalFile(new Path(localInput + inputFile), dsfInputPath);

		// Input
		FileInputFormat.setInputPaths(job, dsfInputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//MapReduce
		job.setMapperClass(FrameCountMapper.class);
		job.setReducerClass(FrameCountReducer.class);
		job.setJarByClass(FrameCountDriver.class);

		// Output
		File outDir = new File(localOutput);
		outDir.renameTo(new File(Long.toString(System.currentTimeMillis())));
		FileOutputFormat.setOutputPath(job, new Path(localOutput));
		job.setOutputFormatClass(TextOutputFormat.class);		

		// execution
		job.waitForCompletion(true);
	}
}