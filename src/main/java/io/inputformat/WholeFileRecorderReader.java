package io.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

@SuppressWarnings("deprecation")
class PcapFileRecordReader implements RecordReader<NullWritable, Text> {
	
	private FileSplit fileSplit;
	private boolean processed = false;	
	
	public PcapFileRecordReader(FileSplit fileSplit) throws IOException {
		this.fileSplit = fileSplit;
	}
	
	@Override
	public NullWritable createKey() {
		return NullWritable.get();
	}
	
	@Override
	public Text createValue() {
		return new Text();
	}
	
	@Override
	public long getPos() throws IOException {
		return processed ? fileSplit.getLength() : 0;
	}
	
	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}
	
//	@Override
//	public boolean next(NullWritable key, BytesWritable value) throws IOException {
//		if (!processed) {
//			byte[] contents = new byte[(int) fileSplit.getLength()];
//			Path file = fileSplit.getPath();
//			FileSystem fs = file.getFileSystem(conf);
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
	public boolean next(NullWritable key, Text value) throws IOException {
		if (!processed) {			
			value.set(fileSplit.getPath().toString());
			processed = true;
			return true;
		}
		return false;
	}
	
	@Override
	public void close() throws IOException {
	}
}
