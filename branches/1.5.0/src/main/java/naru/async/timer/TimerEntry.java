package naru.async.timer;

import java.util.Comparator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import naru.async.Timer;
import naru.async.pool.PoolBase;

public class TimerEntry {
	private static Logger logger=Logger.getLogger(TimerEntry.class);
	private static LinkedList<TimerEntry> pool=new LinkedList<TimerEntry>();
	private static int idsec=0;
	private static final int MAX_POOL=16;
	public static TimerTimeComparator timerTimeComparator=new TimerTimeComparator();
	
	public static TimerEntry getEntry(){
		TimerEntry timerEntry;
		synchronized(pool){
			if(pool.size()==0){
				timerEntry=new TimerEntry();
			}else{
				timerEntry=pool.removeFirst();
			}
			timerEntry.id=idsec;
			idsec++;
		}
		return timerEntry;
	}
	private static void poolEntry(TimerEntry timerEntry){
		synchronized(pool){
			if(pool.size()>=MAX_POOL){
				return;
			}else{
				pool.add(timerEntry);
			}
		}
	}
	
	
	private long id;
	private long timoutTime;
	private Timer timer;
	private Object userContext;
	
	public static class TimerTimeComparator implements Comparator<TimerEntry>{
		public int compare(TimerEntry o1, TimerEntry o2) {
			TimerEntry t1=(TimerEntry)o1;
			TimerEntry t2=(TimerEntry)o2;
			long diff=t1.timoutTime-t2.timoutTime;
			if(diff!=0){
				return (int)diff;
			}
			return (int)(t1.id-t2.id);
		}
	}
	
	public void setup(long timoutTime,Timer timer,Object userContext){
//		System.out.println("timoutTime:"+timoutTime);
//		logger.debug("timoutTime:"+timoutTime);
		this.timoutTime=timoutTime;
		this.timer=timer;
		this.userContext=userContext;
	}
	
	public long getTime() {
		return timoutTime;
	}
	
	public void callback(){
		try {
			timer.onTimer(userContext);
		} catch (Throwable e) {
			logger.warn("onTimeout return exception.timer:"+timer + ":userContext:"+userContext,e);
		}finally{
			poolEntry(this);
		}
	}
	public long getId() {
		return id;
	}
	
	public void pool(){
		poolEntry(this);
	}
}
