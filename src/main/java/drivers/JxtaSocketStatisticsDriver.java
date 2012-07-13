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

import jxta.jnetpcap.socket.SocketStatisticsMapper;
import jxta.jnetpcap.socket.SocketStatisticsReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

@SuppressWarnings("deprecation")
public class JxtaSocketStatisticsDriver {

	private static String inputDir = "/home/thiago/tmp/_/";	 	

	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {
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
		
		for (int k = 0; k < 30; k++) {
			
			JobConf job = new JobConf(JxtaSocketStatisticsDriver.class);
			job.setJarByClass(SocketStatisticsMapper.class);

			System.out.println("### From: " + inputDir);
			System.out.println("### To: " + dstDir);

			// Input
			Path inputPath = new Path(inputDir);
			FileInputFormat.setInputPaths(job,inputPath);
			job.setInputFormat(PcapInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(SortedMapWritable.class);

			//MapReduce Classes
			job.setMapperClass(SocketStatisticsMapper.class);
			job.setReducerClass(SocketStatisticsReducer.class);
			//		job.setNumReduceTasks(3);

			// Output
			job.setOutputFormat(MultipleFilesOutputFormat.class);			
			long t0,t1;			
			
			Path outputPath = new Path("output/1.9GB." + System.currentTimeMillis());
			FileOutputFormat.setOutputPath(job, outputPath);
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

		//process generated files
		//		updateFiles(outputPath);
	}

	public static void updateFiles(Path outputPath) throws FileNotFoundException, IOException {
		int i = 0;
		long min = Long.MAX_VALUE;
		String line = null;

		File rttFile = new File(outputPath.toUri() + "/" + SocketStatisticsMapper.jxtaRelyRttKey);				
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

		File arrivalFile = new File(outputPath.toUri() + "/" + SocketStatisticsMapper.jxtaArrivalKey);
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

		File reqFile = new File(outputPath.toUri() + "/" + SocketStatisticsMapper.jxtaSocketReqKey);
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

		File remFile = new File(outputPath.toUri() + "/" + SocketStatisticsMapper.jxtaSocketRemKey);
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

	private static class MultipleFilesOutputFormat extends MultipleTextOutputFormat<Text,Text>	{
		protected String generateFileNameForKeyValue(Text key,Text value,String filename){
			return key.toString();
		}
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

		BufferedReader br = new BufferedReader(new FileReader(file));
		br.readLine();
		while((line = br.readLine()) != null) {
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
		br.close();
		pw.close();
	}

}