package jxta.jnetpcap.countuptext;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountUpTextReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException {		
		long bc = 0;
		long pc = 0;
		
		for (Text value : values) {
			String line = value.toString();	
			StringTokenizer token = new StringTokenizer(line);
			if(line.length()<0)
				continue;		       
			bc += Long.parseLong(token.nextToken().trim());
			pc += Long.parseLong(token.nextToken().trim());			
		}
		try {
			StringBuilder str = new StringBuilder();
			str.append(bc);
			str.append(" ");
			str.append(pc);
			context.write(key,new Text(Long.toString(bc) + " " + Long.toString(pc)));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}