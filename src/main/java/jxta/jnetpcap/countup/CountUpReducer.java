package jxta.jnetpcap.countup;

import io.type.LongArrayWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountUpReducer extends Reducer<IntWritable, LongArrayWritable, IntWritable, Text> {

	protected void reduce(IntWritable key, Iterable<LongArrayWritable> values, Context context) throws IOException {		
		long bc = 0;		
		long pc = 0;
		
		for (LongArrayWritable value : values) {
			Writable[] tmp = value.get();
			bc += ((LongWritable)tmp[0]).get();
			pc += ((LongWritable)tmp[1]).get();			
		}
		try {
			StringBuilder str = new StringBuilder();
			str.append(bc);
			str.append(" ");
			str.append(pc);
			context.write(key, new Text(str.toString()));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}