package drivers;

import io.inputformat.PcapInputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;

import jxta.jnetpcap.socket.JxtaSocketPerfMapper;
import jxta.jnetpcap.socket.JxtaSocketPerfReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JxtaSocketPerfDriver {

	private static int executions = 2;
	private static String inputDir = "input/";
	private static String numNodesStr = "";

	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {		

		if(args != null && args.length >= 2){
			executions = Integer.valueOf(args[0]);
			inputDir = args[1];
			if(args.length > 2){
				numNodesStr = args[2];
			}
		}
		
		String jobName = "JxtaSocketPerfDriver - " + executions + "x for " + inputDir + " - " + numNodesStr;
		ArrayList<Long> times = new ArrayList<Long>();
		int k = 0;
		for (; k < executions; k++) {			
			Configuration conf = new Configuration();
			Job job = new Job(conf, jobName);
			job.setJarByClass(JxtaSocketPerfMapper.class);
			
			// Mapper
			job.setMapperClass(JxtaSocketPerfMapper.class);
			FileInputFormat.setInputPaths(job,new Path(inputDir));
			job.setInputFormatClass(PcapInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(SortedMapWritable.class);
			
			// Combiner
//			job.setCombinerClass(JxtaSocketPerfCombiner.class);
			
			// Reducer
			job.setReducerClass(JxtaSocketPerfReducer.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			if(numNodesStr != null && !numNodesStr.equals("")){
				int numNodes = Integer.parseInt(numNodesStr);			
				if(numNodes  > 0){
					Integer numReduceTasks = new Float(numNodes * 0.95).intValue();
					job.setNumReduceTasks(numReduceTasks);
					System.out.println("### Reducers: " + numReduceTasks);
				}	
			}
			Path outputPath = new Path("output/JxtaSocketPerfDriver_" + System.currentTimeMillis());
			FileOutputFormat.setOutputPath(job, outputPath);
			
			// Execution
			long t0,t1;
			t0 = System.currentTimeMillis();
			job.waitForCompletion(true);
			t1 = System.currentTimeMillis();
			times.add(t1 - t0);
			System.out.println("### " + (k + 1) + "-Time: " + (t1-t0));	
		}

//		System.out.println("### Times:");
//		double total = 0;
//		for (Long time : times) {
//			System.out.println(time);
//			total += time;
//		}
//		System.out.println("### MeanTime: " + (total/k));
	}

	@SuppressWarnings("unused")
	private static class MultipleFilesOutputFormat extends MultipleTextOutputFormat<Text,Text>	{
		protected String generateFileNameForKeyValue(Text key,Text value,String filename){
			return key.toString();
		}
	}

	@SuppressWarnings("unused")
	private static void updateFiles(Path outputPath) throws FileNotFoundException, IOException {
		int i = 0;
		long min = Long.MAX_VALUE;
		String line = null;

		File rttFile = new File(outputPath.toUri() + "/" + JxtaSocketPerfMapper.jxtaRelyRttKey);				
		BufferedReader rttBuffer = new BufferedReader(new FileReader(rttFile));        
		while((line = rttBuffer.readLine()) != null) {
			i++;
			if(i == 2){
				long tmp = Long.valueOf(new StringTokenizer(line, " ").nextToken());
				if(tmp < min){
					min = tmp;
				}
			}
		}
		rttBuffer.close();
		System.out.println("### Min: " + min);
		setContents(rttFile,min);

		File arrivalFile = new File(outputPath.toUri() + "/" + JxtaSocketPerfMapper.jxtaArrivalKey);
		i = 0;
		BufferedReader arrivalBuffer = new BufferedReader(new FileReader(arrivalFile));        
		while((line = arrivalBuffer.readLine()) != null) {
			i++;
			if(i == 2){
				long tmp = Long.valueOf(new StringTokenizer(line, " ").nextToken());
				if(tmp < min){
					min = tmp;
				}
			}
		}
		arrivalBuffer.close();
		System.out.println("### Min: " + min);
		setContents(arrivalFile,min);

		File reqFile = new File(outputPath.toUri() + "/" + JxtaSocketPerfMapper.jxtaSocketReqKey);
		i = 0;
		BufferedReader reqBuffer = new BufferedReader(new FileReader(reqFile));        
		while((line = reqBuffer.readLine()) != null) {
			i++;
			if(i == 2){
				long tmp = Long.valueOf(new StringTokenizer(line, " ").nextToken());
				if(tmp < min){
					min = tmp;
				}
			}
		}
		reqBuffer.close();
		System.out.println("### Min: " + min);
		setContents(reqFile,min);

		File remFile = new File(outputPath.toUri() + "/" + JxtaSocketPerfMapper.jxtaSocketRemKey);
		i = 0;
		BufferedReader remBuffer = new BufferedReader(new FileReader(remFile));        
		while((line = remBuffer.readLine()) != null) {
			i++;
			if(i == 2){
				long tmp = Long.valueOf(new StringTokenizer(line, " ").nextToken());
				if(tmp < min){
					min = tmp;
				}
			}
		}
		remBuffer.close();
		System.out.println("### Min: " + min);
		setContents(remFile,min);
	}

	private static void setContents(File file, Long value) throws FileNotFoundException, IOException {
		if (file == null) {
			throw new IllegalArgumentException("File should not be null.");
		}
		if (!file.exists()) {
			throw new FileNotFoundException ("File does not exist: " + file);
		}
		if (!file.isFile()) {
			throw new IllegalArgumentException("Should not be a directory: " + file);
		}
		if (!file.canWrite()) {
			throw new IllegalArgumentException("File cannot be written: " + file);
		}

		String line;
		File tempFile = new File(file.getAbsolutePath() + ".txt");
		PrintWriter pw = new PrintWriter(new FileWriter(tempFile));

		BufferedReader buffer = new BufferedReader(new FileReader(file));
		buffer.readLine();
		while((line = buffer.readLine()) != null) {
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			long tmp = Long.valueOf(tokenizer.nextToken());
			tmp = tmp - value;
			StringBuilder str = new StringBuilder();
			str.append(tmp);
			str.append(" ");
			str.append(tokenizer.nextToken());
			pw.println(str);
			pw.flush();
		}
		buffer.close();
		pw.close();
	}

}