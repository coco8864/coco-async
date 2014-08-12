package naru.async.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.queuelet.Queuelet;
import naru.queuelet.QueueletContext;

public class IOManager implements Queuelet {
	private static Logger logger=Logger.getLogger(IOManager.class);
	private static QueueletContext queueletContext;
	private static SelectorContext selectors[];
	private static SelectorStastics stastics[];
	
	public static SelectorContext getSelectorContext(ChannelContext context){
		int index=(int)(context.getPoolId()%selectors.length);
		return selectors[index];
	}
	public static SelectorStastics[] getSelectorStasticses(){
		return stastics;
	}
	
	/**
	 * queuelet　terminalへのキューイング
	 * @author Naru
	 *
	 */
	public static void enqueue(Object obj){
		if(obj==null){
			return;
		}
		queueletContext.enque(obj);
	}
	
	private static String STOP_REQUEST="stop";
	public static void stop(){
		enqueue(STOP_REQUEST);
	}
	
	/* (非 Javadoc)
	 * @see naru.quelet.Quelet#init()
	 */
	public void init(QueueletContext context,Map param) {
		this.queueletContext=context;
		String selectorCountParam=(String)param.get("selectorCount");
		int selectorCount=4;
		if(selectorCountParam!=null){
			selectorCount=Integer.parseInt(selectorCountParam);
		}
		logger.info("selectorCount:"+selectorCount);
		long selectInterval=60000;//selectがタイムアウトする最大時間、実際にはtimeout時間で調整する
		String selectIntervalParam=(String)param.get("selectInterval");
		if(selectIntervalParam!=null){
			selectInterval=Long.parseLong(selectIntervalParam);
		}
		logger.info("selectInterval:"+selectInterval);
		selectors=new SelectorContext[selectorCount];
		stastics=new SelectorStastics[selectorCount];
		try {
			for(int i=0;i<selectors.length;i++){
				selectors[i]=new SelectorContext(i,selectInterval);
				stastics[i]=selectors[i].getStastics();
				selectors[i].start(i);
			}
		} catch (IOException e) {
			logger.error("fail to new SelectorContext.",e);
			context.finish();
		}
	}

	/* (非 Javadoc)
	 * @see naru.quelet.Quelet#term()
	 */
	public void term() {
		for(int i=0;i<selectors.length;i++){
			selectors[i].stop();
		}
	}
	
	public boolean service(Object req) {
		if(req==STOP_REQUEST){
			logger.info("recive stop request");
			for(int i=0;i<selectors.length;i++){
				selectors[i].stop();
				selectors[i].wakeup();
			}
			return false;
		}
		ChannelIO channelIO=(ChannelIO)req;
		try{
			channelIO.doIo();
			return false;
		}finally{
			//IOが完了するまでChannelContextが再利用されないようにする
			context.unref();
		}
	}
	
}
