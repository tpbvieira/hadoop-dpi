package drivers;

import io.inputformat.PcapInputFormat;
import io.type.LongArrayWritable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import jxta.jnetpcap.countup.CountUpMapper;
import jxta.jnetpcap.countup.CountUpReducer;
import jxta.jnetpcap.socket.JxtaSocketPerfMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

@SuppressWarnings("deprecation")
public class CountUpDriver {

	private static int executions = 1;
	private static String inputDir = "/home/thiago/tmp/_/";	

	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {

		if(args != null && args.length == 2){
			executions = Integer.valueOf(args[0]);
			inputDir = args[1];
		}

		Configuration conf = new Configuration();
		ArrayList<Long> times = new ArrayList<Long>();

		// Copy local files into HDFS
		String[] files = new File(inputDir).list();
		FileSystem hdfs = FileSystem.get(conf);
		String dstDir = hdfs.getWorkingDirectory() + "/input/";
		for (int i = 0; i < files.length; i++) {
			Path srcFilePath = new Path(inputDir + files[i]);		
			hdfs.copyFromLocalFile(srcFilePath, new Path(dstDir + files[i]));
		}
		inputDir = dstDir;

		for (int k = 0; k < executions; k++) {			
			JobConf job = new JobConf(CountUpDriver.class);
			job.setJarByClass(JxtaSocketPerfMapper.class);

			// Input
			FileInputFormat.setInputPaths(job,new Path(inputDir));
			job.setInputFormat(PcapInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongArrayWritable.class);

			//MapReduce Classes
			job.setMapperClass(CountUpMapper.class);
			job.setReducerClass(CountUpReducer.class);

			// Output
			job.setOutputFormat(TextOutputFormat.class);			

			Path outputPath = new Path("output/1.9GB." + System.currentTimeMillis());
			FileOutputFormat.setOutputPath(job, outputPath);
			long t0,t1;
			t0 = System.currentTimeMillis();
			JobClient.runJob(job);
			t1 = System.currentTimeMillis();
			times.add(t1-t0);
			System.out.println("### " + k + "-Time: " + (t1-t0));	
		}

		System.out.println("### Times:");
		for (Long time : times) {
			System.out.println(time);
		}

	}
}