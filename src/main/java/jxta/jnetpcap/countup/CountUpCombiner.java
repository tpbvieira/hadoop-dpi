package jxta.jnetpcap.countup;

import io.type.LongArrayWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountUpCombiner extends Reducer<IntWritable, LongArrayWritable, IntWritable, LongArrayWritable> {

	public void reduce(IntWritable key, Iterable<LongArrayWritable> values, Context context) throws IOException {		
		long bc = 0;
		long pc = 0;
		for (LongArrayWritable value : values) {
			Writable[] tmp = value.get();
			bc += Long.parseLong(((LongWritable)tmp[0]).toString());
			pc += Long.parseLong(((LongWritable)tmp[1]).toString());
		}	
				
		LongWritable[] results = new LongWritable[2];
		results[0] = new LongWritable(bc);
		results[1] = new LongWritable(pc);		
		try {
			context.write(key, new LongArrayWritable(results));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}