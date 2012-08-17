package drivers;

import io.inputformat.PcapInputFormat;

import java.io.IOException;
import java.util.ArrayList;

import jxta.jnetpcap.countup.CountUpMapper;
import jxta.jnetpcap.payloadcount.PayloadCountMapper;
import jxta.jnetpcap.payloadcount.PayloadCountReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PayloadCountDriver {

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
			job.setMapperClass(PayloadCountMapper.class);
			FileInputFormat.setInputPaths(job, new Path(inputDir));
			job.setInputFormatClass(PcapInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			// Reducer
			job.setReducerClass(PayloadCountReducer.class);
			Path outputPath = new Path("output/PayloadCountDriver_" + System.currentTimeMillis());
			FileOutputFormat.setOutputPath(job, outputPath);
			job.setOutputFormatClass(TextOutputFormat.class);		

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