package naru.async.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import org.apache.log4j.Logger;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
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
	
	private enum EnqueueType {
		Read,
		Connect,
		Write,
		Close,
		StopQueue
	}
	
	public static class Enqueue extends PoolBase{
		private EnqueueType type;
		private ChannelContext context;
	}
	
	private static void enqueue(EnqueueType type,ChannelContext context){
		Enqueue enqueue=(Enqueue)PoolManager.getInstance(Enqueue.class);
		enqueue.type=type;
		enqueue.context=context;
		if(context!=null){
			context.ref();
		}
		queueletContext.enque(enqueue);
	}
	
	public static void enqueueRead(Object obj){
		enqueue(EnqueueType.Read,(ChannelContext)obj);
	}
	
	public static void enqueueWrite(Object obj){
		enqueue(EnqueueType.Write,(ChannelContext)obj);
	}
	
	public static void enqueueClose(Object obj){
		enqueue(EnqueueType.Close,(ChannelContext)obj);
	}
	
	public static void enqueueConnect(Object obj){
		enqueue(EnqueueType.Connect,(ChannelContext)obj);
	}
	
	public static void stop(){
		enqueue(EnqueueType.StopQueue,null);
	}
	
	/* (�� Javadoc)
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
		long selectInterval=60000;//select���^�C���A�E�g����ő厞�ԁA���ۂɂ�timeout���ԂŒ�������
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

	/* (�� Javadoc)
	 * @see naru.quelet.Quelet#term()
	 */
	public void term() {
		for(int i=0;i<selectors.length;i++){
			selectors[i].stop();
		}
	}
	
	private void executeRead(ChannelContext context) {
		context.prepareRead();
		ReadableByteChannel channel=(ReadableByteChannel)context.getChannel();
		ByteBuffer buffer=PoolManager.getBufferInstance();
		long length=0;
		Throwable failure=null;
		try {
			length=channel.read(buffer);
			buffer.flip();
			logger.debug("##executeRead length:"+length +":cid:"+context.getPoolId());
			//logger.debug("%%%%%%%%%%%%");
			//logger.debug(new String(buffer.array(),0,buffer.limit()));
			//logger.debug("%%%%%%%%%%%%");
		} catch (IOException e) {
			failure=e;
			logger.warn("fail to read.cid:"+context.getPoolId() +":channel:"+ channel,failure);
		} catch (Throwable e) {
			failure=e;
			logger.warn("fail to read Throwable.cid:"+context.getPoolId() +":channel:"+ channel,failure);
		}
		if(failure!=null){
			PoolManager.poolBufferInstance(buffer);
			context.disconnect();
			if( context.failure(failure) ){
				logger.warn("fail to read closable");
			}
			context.dump();
			return;
		}
		
		ByteBuffer[] buffers=null;
		if(length<=0){
			logger.debug("executeRead cid:"+context.getPoolId()+":length:"+length+":channel:"+channel+":buffer:"+buffer);
			context.dump();
			PoolManager.poolBufferInstance(buffer);
		}else{
			buffers=BuffersUtil.toByteBufferArray(buffer);
		}
		context.doneRead(buffers);
	}
	
	private boolean executeWrite(ChannelContext context) {
		Throwable failure=null;
		GatheringByteChannel channel=null;
		long length=0;
		ByteBuffer[] prepareBuffers=null;
		boolean isWriteExecute=true;
		try {
			prepareBuffers=context.prepareWrite();
			if(prepareBuffers==null){
				isWriteExecute=false;
				return false;//swap�@out����Ă��Ă�����buffer���p�ӂł��Ȃ������B��蒼��
			}
			channel=(GatheringByteChannel)context.getChannel();
			length=channel.write(prepareBuffers);
			logger.debug("##executeWrite length:"+length +":cid:"+context.getPoolId());
			//length��0�̎�������炵��,buffers�̒�����0�ɈႢ�Ȃ�,buffer�ė��p�̖�肩�H
			if(logger.isDebugEnabled()){
				logger.debug("executeWrite."+length +":cid:"+context.getPoolId()+":channel:"+channel);
				if(length==0){
					for(int i=0;i<prepareBuffers.length;i++){
						logger.error("0 buffer.array:"+prepareBuffers[i].array());
					}
					logger.error("buffers length:"+BuffersUtil.remaining(prepareBuffers));
				}
			}
		} catch (IOException e) {
			logger.warn("fail to write.channel:"+channel+":cid:"+context.getPoolId(),e);
			failure=e;
		} catch (Throwable e) {
			logger.warn("fail to write. Throwable channel:"+channel+":cid:"+context.getPoolId(),e);
			failure=e;
		}finally{
			if(failure==null){
				if(isWriteExecute){
					context.doneWrite(prepareBuffers,length);
				}
			}else{
				if(prepareBuffers!=null){
					PoolManager.poolArrayInstance(prepareBuffers);
				}
				context.disconnect();
				if( context.failure(failure) ){
					logger.warn("fail to write closable");
//					context.doneClosed(false);
				}
			}
		}
		return true;
	}
	
	private boolean executeConnect(ChannelContext context) {
		context.prepareIO(ChannelContext.SelectState.CONNECTING);
		try {
			context.finishConnect();
			context.queueuSelect();//queueselect�ɒu��
			return true;
		} catch (IOException e) {
			logger.warn("connect error", e);
			context.failure(e);
		}
		return false;
	}
	
	public boolean service(Object req) {
		Enqueue enqueue=(Enqueue)req;
		ChannelContext context=enqueue.context;
		try{
			switch(enqueue.type){
			case StopQueue:
				logger.info("recive stop request");
				for(int i=0;i<selectors.length;i++){
					selectors[i].stop();
					selectors[i].wakeup();
				}
				return false;
			case Read:
				executeRead(context);
				break;
			case Connect:
				executeConnect(context);
				break;
			case Write:
				executeWrite(context);
				break;
			case Close:
				context.prepareIO(ChannelContext.SelectState.CLOSEING);
				context.doneClosed(false);//�v���ɂ��close����ꍇ
				break;
			}
		}finally{
			//IO����������܂�ChannelContext���ė��p����Ȃ��悤�ɂ���
			if(context!=null){
				context.unref();
			}
		}
		return false;
	}
	
}