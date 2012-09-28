package jxta.jnetpcap.socket;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class JxtaSocketPerfReducer extends Reducer<Text, SortedMapWritable, LongWritable, LongWritable> {

	@SuppressWarnings("rawtypes")
	@Override
	public void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException {		

		if(key.equals(JxtaSocketPerfMapper.jxtaArrivalKey)){			
			SortedMapWritable tmp = null;
			
			for (SortedMapWritable value : values) {				
				tmp = value;
			}
			Set<WritableComparable> arrivalsKeys = tmp.keySet();
			for (WritableComparable arrivalKey : arrivalsKeys) {
				try {
					context.write((LongWritable)arrivalKey, (LongWritable)tmp.get(arrivalKey));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			tmp.clear();
		}else
			if(key.equals(JxtaSocketPerfMapper.jxtaRelyRttKey)){
				SortedMapWritable tmp = null;
				for (SortedMapWritable value : values) {
					tmp = value;
				}
				Set<WritableComparable> rttKeys = tmp.keySet();
				for (WritableComparable rttKey : rttKeys) {
					ArrayWritable rttsArray = (ArrayWritable)tmp.get(rttKey);
					Writable[] rtts = rttsArray.get();						
					for (int j = 0; j < rtts.length; j++) {
						try {
							context.write((LongWritable)rttKey, (LongWritable)rtts[j]);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
					
				tmp.clear();
			}else
				if(key.equals(JxtaSocketPerfMapper.jxtaSocketReqKey)){			
					SortedMapWritable tmp = null;
					for (SortedMapWritable value : values) {
						tmp = value;
					}
					Set<WritableComparable> requestsKeys = tmp.keySet();
					for (WritableComparable requestKey : requestsKeys) {
						try {
							context.write((LongWritable)requestKey, (LongWritable)tmp.get(requestKey));
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					tmp.clear();
				}else
					if(key.equals(JxtaSocketPerfMapper.jxtaSocketRemKey)){			
						SortedMapWritable tmp = null;
						for (SortedMapWritable value : values) {
							tmp = value;
						}
						Set<WritableComparable> responsesKeys = tmp.keySet();
						for (WritableComparable responseKey : responsesKeys) {							
							try {
								context.write((LongWritable)responseKey, (LongWritable)tmp.get(responseKey));
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}						
						tmp.clear();
					}

	}
}