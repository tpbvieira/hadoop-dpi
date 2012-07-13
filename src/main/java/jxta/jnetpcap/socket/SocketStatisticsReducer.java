package jxta.jnetpcap.socket;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class SocketStatisticsReducer extends MapReduceBase implements Reducer<Text, SortedMapWritable, Text, Text> {

	@SuppressWarnings("rawtypes")
	@Override
	public void reduce(Text key, Iterator<SortedMapWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		

		StringBuilder strOutput = new StringBuilder();

		if(key.equals(SocketStatisticsMapper.jxtaArrivalKey)){// Arrival count			
			SortedMapWritable tmp = null;
			while(values.hasNext()){
				tmp = values.next();
			}
			Set<WritableComparable> arrivals = tmp.keySet();
			for (WritableComparable arrivalTime : arrivals) {
				strOutput.append("\n");
				strOutput.append((LongWritable)arrivalTime);
				strOutput.append(" ");
				strOutput.append(((LongWritable)tmp.get(arrivalTime)).get());		
			}
			output.collect(new Text(key.toString()), new Text(strOutput.toString()));
			tmp.clear();
		}else
			if(key.equals(SocketStatisticsMapper.jxtaRelyRttKey)){// RTT
				SortedMapWritable tmp = null;
				while(values.hasNext()){
					tmp = values.next();
				}
				Set<WritableComparable> times = tmp.keySet();
				for (WritableComparable time : times) {
					ArrayWritable rttsArray = (ArrayWritable)tmp.get(time);
					Writable[] rtts = rttsArray.get();						
					for (int j = 0; j < rtts.length; j++) {
						strOutput.append("\n");
						strOutput.append(time);
						strOutput.append(" ");
						strOutput.append(((LongWritable)rtts[j]).get());
					}
				}
				output.collect(new Text(key.toString()), new Text(strOutput.toString()));	
				tmp.clear();
			}else
				if(key.equals(SocketStatisticsMapper.jxtaSocketReqKey)){// Socket request count			
					SortedMapWritable tmp = null;
					while(values.hasNext()){
						tmp = values.next();
					}
					Set<WritableComparable> requests = tmp.keySet();
					for (WritableComparable requestTime : requests) {
						strOutput.append("\n");
						strOutput.append((LongWritable)requestTime);
						strOutput.append(" ");
						strOutput.append(((LongWritable)tmp.get(requestTime)).get());		
					}
					output.collect(new Text(key.toString()), new Text(strOutput.toString()));
					tmp.clear();
				}else
					if(key.equals(SocketStatisticsMapper.jxtaSocketRemKey)){// Socket response count			
						SortedMapWritable tmp = null;
						while(values.hasNext()){
							tmp = values.next();
						}
						Set<WritableComparable> responses = tmp.keySet();
						for (WritableComparable responseTime : responses) {
							strOutput.append("\n");
							strOutput.append((LongWritable)responseTime);
							strOutput.append(" ");
							strOutput.append(((LongWritable)tmp.get(responseTime)).get());		
						}
						output.collect(new Text(key.toString()), new Text(strOutput.toString()));
						tmp.clear();
					}

	}
}