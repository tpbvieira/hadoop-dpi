package io.inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class PcapInputFormat extends FileInputFormat<NullWritable, Text> {

	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	public RecordReader<NullWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException{		
		final FileSplit file = (FileSplit)split;
		reporter.setStatus(file.getPath().toString());
		return new PcapFileRecordReader(file);
	}

}