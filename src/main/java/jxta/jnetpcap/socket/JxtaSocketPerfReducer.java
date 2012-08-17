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
		System.out.println("### JxtaSocketPerfReducer");
		if(key.equals(JxtaSocketPerfMapper.jxtaArrivalKey)){// Arrival count			
			SortedMapWritable tmp = null;
			
			for (SortedMapWritable value : values) {
				System.out.println("###  " + key + " - " + value.size());
				tmp = value;
			}
			Set<WritableComparable> arrivals = tmp.keySet();
			for (WritableComparable arrivalTime : arrivals) {
				try {
					context.write((LongWritable)arrivalTime, (LongWritable)tmp.get(arrivalTime));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			tmp.clear();
		}else
			if(key.equals(JxtaSocketPerfMapper.jxtaRelyRttKey)){// RTT
				SortedMapWritable tmp = null;
				for (SortedMapWritable value : values) {
					System.out.println("###  " + key + " - " + value.size());
					tmp = value;
				}
				Set<WritableComparable> times = tmp.keySet();
				for (WritableComparable time : times) {
					ArrayWritable rttsArray = (ArrayWritable)tmp.get(time);
					Writable[] rtts = rttsArray.get();						
					for (int j = 0; j < rtts.length; j++) {
						try {
							context.write((LongWritable)time, (LongWritable)rtts[j]);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
					
				tmp.clear();
			}else
				if(key.equals(JxtaSocketPerfMapper.jxtaSocketReqKey)){// Socket request count			
					SortedMapWritable tmp = null;
					for (SortedMapWritable value : values) {
						System.out.println("###  " + key + " - " + value.size());
						tmp = value;
					}
					Set<WritableComparable> requests = tmp.keySet();
					for (WritableComparable requestTime : requests) {
						try {
							context.write((LongWritable)requestTime, (LongWritable)tmp.get(requestTime));
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					tmp.clear();
				}else
					if(key.equals(JxtaSocketPerfMapper.jxtaSocketRemKey)){// Socket response count			
						SortedMapWritable tmp = null;
						for (SortedMapWritable value : values) {
							System.out.println("###  " + key + " - " + value.size());
							tmp = value;
						}
						Set<WritableComparable> responses = tmp.keySet();
						for (WritableComparable responseTime : responses) {							
							try {
								context.write((LongWritable)responseTime, (LongWritable)tmp.get(responseTime));
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}						
						tmp.clear();
					}

	}
}