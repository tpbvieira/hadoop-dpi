package jxta.jnetpcap.socket;

import java.util.SortedMap;

/**
 * 
 * 
 * @author Thiago Vieira
 *
 */

public class JxtaSocketStatistics {	
	private long payload;
	private long startTime;
	private long endTime;
	private SortedMap<Integer,Long> rtts;
	
	public JxtaSocketStatistics(long payload, long startTime, long endTime, SortedMap<Integer,Long> rtts){
		this.payload = payload;
		this.endTime = startTime;
		this.endTime = endTime;
		this.rtts = rtts;
	}
	
	public long getPayload() {
		return payload;
	}	
	
	public long getTransferTime() {
		return endTime - startTime;
	}
	
	public double getThroughput(){
		double a = payload;
		double b = endTime - startTime;
		
		return a/b;
	}
	
	public long getStartTime(){
		return startTime;
	}
	
	public long getEndTime(){
		return endTime;
	}
	
	public SortedMap<Integer,Long> getRtts(){
		return rtts;
	}
	
	public void setRtts(SortedMap<Integer,Long> rtts){
		this.rtts = rtts;
	}
}