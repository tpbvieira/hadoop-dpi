package drivers;

import io.inputformat.PcapInputFormat;
import io.type.LongArrayWritable;

import java.io.IOException;

import jxta.jnetpcap.countup.CountUpMapper;
import jxta.jnetpcap.countuptext.CountUpTextMapper;
import jxta.jnetpcap.countuptext.CountUpTextReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountUpText {

	private static int executions = 1;
	private static String inputDir = "input/";
	private static String numNodes = "";

	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {

		if(args != null && args.length >= 2){
			executions = Integer.valueOf(args[0]);
			inputDir = args[1];
			if(args.length > 2){
				numNodes = args[2];
			}
		}
		
		String jobName = "CountUpText - " + executions + "x for " + inputDir + " - " + numNodes;		
		for (int k = 0; k < executions; ) {			
			Configuration conf = new Configuration();
			Job job = new Job(conf, jobName);
			job.setJarByClass(CountUpMapper.class);

			// Mapper
			job.setMapperClass(CountUpTextMapper.class);
			Path inputPath = new Path(inputDir);
			FileInputFormat.setInputPaths(job, inputPath);
			job.setInputFormatClass(PcapInputFormat.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(LongArrayWritable.class);
						
			// Combiner
			job.setCombinerClass(CountUpTextReducer.class);

			// Reducer			
			job.setReducerClass(CountUpTextReducer.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			if(numNodes != null && numNodes.length() > 0){
				Integer nodes = Integer.decode(numNodes);
				Integer numReduceTasks = new Float(nodes * 0.95).intValue();
				job.setNumReduceTasks(numReduceTasks);
			}
			Path outputPath = new Path("output/CountUpDriver_" + System.currentTimeMillis());
			FileOutputFormat.setOutputPath(job, outputPath);
			
			// Execution
			long t0,t1;
			t0 = System.currentTimeMillis();
			job.waitForCompletion(true);
			t1 = System.currentTimeMillis();
			System.out.println("### " + (++k) + "-Time: " + (t1-t0));	
		}
	}
}