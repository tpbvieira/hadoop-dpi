package drivers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import jxta.jnetpcap.payloadcount.PayloadCountMapper;
import jxta.jnetpcap.payloadcount.PayloadCountReducer;

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

public class PayloadCountDriver {

	private static final String pcapPath = "/home/thiago/tmp/_/";
	private static final String inputPath = "input/";
	private static final String outputPath = "output/";
	
	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "PayloadCountDriver");

		File dir = new File(pcapPath);
		String[] fileList = dir.list();

		// Generate input with file's list and Copy local files into HDFS
		PrintWriter file = new PrintWriter(new BufferedWriter(new FileWriter(inputPath + "fileList.txt")));
		FileSystem hdfs = FileSystem.get(conf);
		Path dstPath = new Path(hdfs.getWorkingDirectory() + "/");
		for (int i = 0; i < fileList.length; i++) {
			file.println(fileList[i]);
			Path srcPath = new Path(pcapPath + fileList[i]);		
			hdfs.copyFromLocalFile(srcPath, dstPath);
		}
		file.close();
		
		// Input
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//MapReduce
		job.setMapperClass(PayloadCountMapper.class);
		job.setReducerClass(PayloadCountReducer.class);

		// Output
		File outDir = new File(outputPath);
		outDir.renameTo(new File(Long.toString(System.currentTimeMillis())));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setOutputFormatClass(TextOutputFormat.class);		

		job.waitForCompletion(true);
	}
}