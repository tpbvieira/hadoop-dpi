package io.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class PcapFileRecordReader extends RecordReader<NullWritable, Text> {

	private FileSplit fileSplit;
	private Text value;
	private boolean processed = false;	

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

//	public boolean next(NullWritable key, BytesWritable value) throws IOException {
//		if (!processed) {
//			byte[] contents = new byte[(int) fileSplit.getLength()];
//			Path file = fileSplit.getPath();
//			FileSystem fs = file.getFileSystem(null);
//			FSDataInputStream in = null;
//			try {
//				in = fs.open(file);
//				IOUtils.readFully(in, contents, 0, contents.length);
//				value.set(contents, 0, contents.length);
//			} finally {
//				IOUtils.closeStream(in);
//			}
//			processed = true;
//			return true;
//		}
//		return false;
//	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public NullWritable getCurrentKey() throws IOException,	InterruptedException {
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit)split;		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {			
			value = new Text(fileSplit.getPath().toString());
			processed = true;
			return true;
		}
		return false;
	}
}