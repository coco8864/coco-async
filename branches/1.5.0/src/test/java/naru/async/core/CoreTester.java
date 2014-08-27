package naru.async.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import naru.async.BuffersTester;

public class CoreTester {
	private List<BuffersTester> testers=new ArrayList<BuffersTester>();
	private Set<Long>cids=new HashSet<Long>();
	private int outTestCounter=0;
	private long startTime;
	private int readCount=0;
	private int writtenCount=0;
	private int closedCount=0;
	private int timeoutCount=0;
	private int failureCount=0;
	private int connectFailureCount=0;
	
	public CoreTester(){
		startTime=System.currentTimeMillis();
	}
	
	public synchronized void intoTest(TestHandler handler){
		long cid=handler.getChannelId();
		handler.setBufferTester(new BuffersTester(cid));
		cids.add(cid);
	}
	
	
	public synchronized void outTest(TestHandler handler){
		cids.remove(handler.getChannelId());
		testers.add(handler.getBufferTester());
		outTestCounter++;
		readCount+=handler.getReadCount();
		writtenCount+=handler.getWrittenCount();
		closedCount+=handler.getClosedCount();
		connectFailureCount+=handler.getConnectFailureCount();
		failureCount+=handler.getFailureCount();
		timeoutCount+=handler.getTimeoutCount();
		notify();
	}
	
	public void waitAndCheck(int channelCount) {
		synchronized(this){
			while(outTestCounter<(channelCount-connectFailureCount)){
				try {
					this.wait(1000);
				} catch (InterruptedException e) {
				}
				System.out.println("cids["+cids.size() + "]"+cids);
			}
		}
		System.out.println("CoreTester time:"+(System.currentTimeMillis()-startTime));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
		}
		StringBuilder sb=new StringBuilder();
		sb.append("readCount:");
		sb.append(readCount);
		sb.append(":writtenCount:");
		sb.append(writtenCount);
		sb.append(":closedCount:");
		sb.append(closedCount);
		sb.append(":connectFailureCount:");
		sb.append(connectFailureCount);
		sb.append(":failureCount:");
		sb.append(failureCount);
		sb.append(":timeoutCount:");
		sb.append(timeoutCount);
		System.out.println(sb.toString());
		assertEquals((channelCount-connectFailureCount), outTestCounter);
		for(BuffersTester tester:testers){
			if(tester==null){
				System.out.println("CoreTester#waitAndCheck tester is null");
				continue;
			}
			System.out.println(tester.getLength());
			try{
				tester.check();
			}catch(Throwable e){
				e.printStackTrace();
			}
		}
	}

}
