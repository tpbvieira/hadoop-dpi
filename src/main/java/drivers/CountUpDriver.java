package drivers;

import io.inputformat.PcapInputFormat;
import io.type.LongArrayWritable;

import java.io.IOException;
import java.util.ArrayList;

import jxta.jnetpcap.countup.CountUpCombiner;
import jxta.jnetpcap.countup.CountUpMapper;
import jxta.jnetpcap.countup.CountUpReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountUpDriver {

	private static int executions = 1;
	private static String inputDir = "input/";	

	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {
		// Arguments
		if(args != null && args.length == 2){
			executions = Integer.valueOf(args[0]);
			inputDir = args[1];
		}

		ArrayList<Long> times = new ArrayList<Long>();
		for (int k = 0; k < executions; ) {			
			Configuration conf = new Configuration();
			Job job = new Job(conf, "CountUpDriver");
			job.setJarByClass(CountUpMapper.class);

			// Mapper
			job.setMapperClass(CountUpMapper.class);
			Path inputPath = new Path(inputDir);
			FileInputFormat.setInputPaths(job, inputPath);
			job.setInputFormatClass(PcapInputFormat.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(LongArrayWritable.class);
						
			// Combiner
			job.setCombinerClass(CountUpCombiner.class);

			// Reducer			
			job.setReducerClass(CountUpReducer.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			Path outputPath = new Path("output/CountUpDriver_" + System.currentTimeMillis());
			FileOutputFormat.setOutputPath(job, outputPath);
			
			// Execution
			long t0,t1;
			t0 = System.currentTimeMillis();
			job.waitForCompletion(true);
			t1 = System.currentTimeMillis();
			times.add(t1 - t0);
			System.out.println("### " + (++k) + "-Time: " + (t1-t0));	
		}
		System.out.println("### Times:");
		for (Long time : times) {
			System.out.println(time);
		}
	}
}