package jxta.jnetpcap.countup;

import io.type.LongArrayWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


@SuppressWarnings("deprecation")
public class CountUpReducer extends MapReduceBase implements Reducer<Text, LongArrayWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<LongArrayWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		long bc = 0;
		long pc = 0;

		while(values.hasNext()){
			Writable[] tmp = values.next().get();
			bc += Long.parseLong(((LongWritable)tmp[0]).toString());
			pc += Long.parseLong(((LongWritable)tmp[1]).toString());
		}
		output.collect(key, new Text(Long.toString(bc) + " " + Long.toString(pc)));    
		
	}
}