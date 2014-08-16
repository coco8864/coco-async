package naru.async.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import naru.async.ChannelHandler;
import naru.async.ChannelStastics;
import naru.async.core.Order.OrderType;
import naru.async.core.SelectOperator.State;
import naru.async.pool.BuffersUtil;

import org.apache.log4j.Logger;
/**
 * 
 * ChannelContextが保持するOrderを管理
 * 排他は、ChannelContext側で獲る
 * @author naru
 *
 */
public class OrderOperator {
	private static Logger logger=Logger.getLogger(OrderOperator.class);
	private static final long CLOSE_INTERVAL=10000;
	private static class DummyHandler extends ChannelHandler{
		@Override
		public void onFinished() {
		}
	}
	private static final DummyHandler DUMMY_HANDLER=new DummyHandler();
	
	private Order acceptOrder;
	private Order connectOrder;
	private Order readOrder;
	private LinkedList<Order> writeOrders=new LinkedList<Order>();
	private Order closeOrder;
	private Throwable failure;
	private boolean isFinished;//finish　Orderを実行したか否か
	
	private ChannelContext context;
	private ChannelStastics stastics;
	private SelectOperator selectOperator;
	private WriteOperator writeOperator;

	public OrderOperator(ChannelContext context){
		this.context=context;
	}
	
	void setup(){
		this.stastics=context.getChannelStastics();
		this.writeOperator=context.getWriteOperator();
		this.selectOperator=context.getSelectOperator();
		acceptOrder=null;
		connectOrder=null;
		readOrder=null;
		writeOrders.clear();
		closeOrder=null;
		failure=null;
		isFinished=false;
	}
	
	/* callback関連 */
	//private static ThreadLocal<ChannelContext> currentContext=new ThreadLocal<ChannelContext>();
	private LinkedList<Order> callbackOrders=new LinkedList<Order>();
	private boolean inCallback=false;
	public synchronized void queueCallback(Order order){
		callbackOrders.add(order);
		logger.debug("queueCallback cid:"+context.getPoolId() +":size:"+callbackOrders.size()+":"+order.getOrderType());
		if(inCallback==false){
			inCallback=true;
			DispatchManager.enqueue(this);
		}
	}
	
	synchronized boolean checkAndCallbackFinish(){
		logger.debug("checkAndCallbackFinish.cid:"+context.getPoolId());
		if(isFinished){
			return false;
		}
		if(orderCount()!=0){
			return false;
		}
		if(!selectOperator.isClose()){
			return false;
		}
		if(!writeOperator.isClose()){
			return false;
		}
		isFinished=true;
		logger.debug("checkAndCallbackFinish done.cid:"+context.getPoolId());
		queueCallback(Order.create(context.getHandler(), OrderType.finish, null));
		return true;
	}
	
	private synchronized Order popOrder(){
		logger.debug("callback cid:"+context.getPoolId() +":size:"+callbackOrders.size());
		if(callbackOrders.size()==0){
			inCallback=false;
			return null;
		}
		Order order=(Order)callbackOrders.remove(0);
		return order;
	}
	
	/**
	 * dispatch workerから呼び出される、多重では走行しない
	 */
	public void callback(){
		Order order=null;
		boolean isFinishCallback=false;
		ChannelHandler finishHandler=null;
		try{
			while(true){
				order=popOrder();
				if(order==null){
					break;
				}
				if(order.isFinish()){
					isFinishCallback=true;
					finishHandler=order.getHandler();
				}
				try{
					order.callback(stastics);
				}catch(Throwable t){
					logger.warn("callback return Throwable",t);
					//TODO 回線が切断されたとして処理
					//doneClosed(true);
				}
			}
		}finally{
			synchronized(context){
				checkAndCallbackFinish();
			}
			//currentContext.set(null);
			if(isFinishCallback==false){
				return;
			}
			logger.debug("callback isFinishCallback=true.cid:"+context.getPoolId());
			//if(finishHandler!=handler){
			//	logger.warn("finish callback finishHandler:"+finishHandler);
			//	logger.warn("finish callback handler:"+handler);
			//}
			finishHandler.unref();
			context.setHandler(null);
			context.unref();
		}
	}
	
	public void dump(){
		dump(logger);
	}
	public void dump(Logger logger){
		//writeOrdersは、実体をダンプするとConcurrentModificationExceptionとなる。
		StringBuffer sb=new StringBuffer();
		sb.append("$writeOrders.size:");
		sb.append(writeOrders.size());
		sb.append(":acceptOrder:");
		sb.append(acceptOrder);
		sb.append(":connectOrder:");
		sb.append(connectOrder);
		sb.append(":readOrder:");
		sb.append(readOrder);
		sb.append(":closeOrder:");
		sb.append(closeOrder);
		sb.append(":failure:");
		sb.append(failure);
		logger.debug(sb.toString());
	}
	
	boolean isCloseOrder(){
		return closeOrder!=null;
	}
	
	public boolean isReadOrder(){
		return readOrder!=null;
	}
	
	public boolean isWriteOrder(){
		return writeOrders.size()!=0;
	}
	public boolean isConnectOrder(){
		return connectOrder!=null;
	}
	
	public static final long CHECK_TIMEOUT=-1;
	public static final long CHECK_NO_ORDER=-2;
	
	long checkConnectTimeout(long now){
		if(connectOrder==null){
			return CHECK_NO_ORDER;
		}
		long timeoutTime=connectOrder.getTimeoutTime();
		if(now<timeoutTime){
			return timeoutTime;
		}
		return CHECK_TIMEOUT;
	}
	
	long checkReadTimeout(long now){
		if(readOrder==null){
			return CHECK_NO_ORDER;
		}
		long timeoutTime=readOrder.getTimeoutTime();
		if(now<timeoutTime){
			return timeoutTime;
		}
		return CHECK_TIMEOUT;
	}
	
	long checkWriteTimeout(long now){
		if(writeOrders.size()==0){
			return CHECK_NO_ORDER;
		}
		long timeoutTime=Long.MAX_VALUE;
		for(Order order:writeOrders){
			long time=order.getTimeoutTime();
			if(timeoutTime>time){
				timeoutTime=time;
			}
		}
		if(now<timeoutTime){
			return timeoutTime;
		}
		return CHECK_TIMEOUT;
	}
	
	public int orderCount(){
		int count=0;
		if(acceptOrder!=null){
			count++;
		}
		if(connectOrder!=null){
			count++;
		}
		if(readOrder!=null){
			count++;
		}
		count+=writeOrders.size();
		if(closeOrder!=null){
			count++;
		}
		return count;
	}
	
	/**
	 * 全Orderにfailure通知
	 */
	int failure(Throwable failure){
		int count=0;
		this.failure=failure;
		if(acceptOrder!=null){
			acceptOrder.setFailure(failure);
			queueCallback(acceptOrder);
			count++;
			acceptOrder=null;
		}
		if(connectOrder!=null){
			connectOrder.setFailure(failure);
			queueCallback(connectOrder);
			count++;
			connectOrder=null;
		}
		if(readOrder!=null){
			readOrder.setFailure(failure);
			queueCallback(readOrder);
			count++;
			readOrder=null;
		}
		for(Order writeOrder:writeOrders){
			writeOrder.setFailure(failure);
			queueCallback(writeOrder);
			count++;
		}
		writeOrders.clear();
		if(closeOrder!=null){
			closeOrder.setFailure(failure);
			queueCallback(closeOrder);
			count++;
			closeOrder=null;
		}
		return count;
	}
	
	
	/**
	 * 全Orderにfailure通知
	 */
	private int closeOrder(){
		int count=0;
		if(acceptOrder!=null){
			acceptOrder.closeOrder();
			queueCallback(acceptOrder);
			count++;
			acceptOrder=null;
		}
		if(connectOrder!=null){
			connectOrder.closeOrder();
			queueCallback(connectOrder);
			count++;
			connectOrder=null;
		}
		if(readOrder!=null){
			readOrder.closeOrder();
			queueCallback(readOrder);
			count++;
			readOrder=null;
		}
		for(Order writeOrder:writeOrders){
			writeOrder.closeOrder();
			queueCallback(writeOrder);
			count++;
		}
		writeOrders.clear();
		if(closeOrder!=null){
			closeOrder.closeOrder();
			queueCallback(closeOrder);
			count++;
			closeOrder=null;
		}
		return count;
	}
	
	
	/**
	 * 指定されたorderにtimeout通知
	 * connectとwriteはtimeoutはfailureーとして扱う
	 * readは復旧可能
	 */
	public void timeout(OrderType orderType){
		switch(orderType){
		case connect:
		case write:
			failure(new Exception("timeout:"+orderType));
			break;
		case read:
			if(readOrder!=null){
				readOrder.timeout();
				queueCallback(readOrder);
				readOrder=null;
			}
			break;
		}
	}
	
	/**
	 * 指定されたorderにclosed通知
	 */
	int doneClose(boolean isAsyncClose){
		int count=0;
		if(closeOrder!=null){
			queueCallback(closeOrder);
			count++;
			closeOrder=null;
		}
		if(isAsyncClose){
			if(selectOperator.isClose()){
				return count;
			}
			/*　正常系ではしばらく待つと0長受信するはず,0長受信を一定期間待つ */
			if(readOrder!=null){
				readOrder.setTimeoutTime(System.currentTimeMillis()+CLOSE_INTERVAL);
			}else{
				readOrder=Order.create(DUMMY_HANDLER, OrderType.read, null);
			}
		}else{
			closeOrder();
		}
		return count;
	}
	
	public boolean doneRead(ArrayList<ByteBuffer> buffers){
		if(readOrder==null || buffers.size()==0){
			return false;
		}
		Order order=readOrder;
		readOrder=null;
		ByteBuffer[] bufs=BuffersUtil.toByteBufferArray(buffers);
		buffers.clear();
		order.setBuffers(bufs);
		queueCallback(order);
		return true;
	}
	
	public boolean doneWrite(long totalWriteLength){
		if(writeOrders.size()==0){
			return false;
		}
		Iterator<Order> itr=writeOrders.iterator();
		while(itr.hasNext()){
			Order order=itr.next();
			if(order.getWriteEndOffset()>totalWriteLength){
				break;
			}
			itr.remove();
			logger.debug("doneWrite queueCallback cid:"+context.getPoolId()+":userContext:"+order.getUserContext());
			queueCallback(order);
		}
		logger.debug("doneWrite cid:"+context.getPoolId()+" writeOrderCount:"+writeOrders.size());
		return true;
	}
	
	public boolean doneConnect(){
		if(connectOrder==null){
			return false;
		}
		queueCallback(connectOrder);
		connectOrder=null;
		return true;
	}
	
	
	boolean acceptOrder(Object userContext){
		if(acceptOrder!=null){
			return false;
		}
		acceptOrder=Order.create(context.getHandler(), OrderType.accept, userContext);
		selectOperator.queueSelect(State.accepting);
		return true;
	}
	
	boolean connectOrder(Object userContext,long timeout){
		if(connectOrder!=null){
			return false;
		}
		connectOrder=Order.create(context.getHandler(), OrderType.connect, userContext);
		if(timeout>0){
			connectOrder.setTimeoutTime(System.currentTimeMillis()+timeout);
		}
		if(context.isConnected()){//すでにconnectが成功していた場合
			queueCallback(connectOrder);
			connectOrder=null;
			selectOperator.queueSelect(State.selectReading);
		}else{
			selectOperator.queueSelect(State.selectConnecting);
		}
		return true;
	}
	
	boolean readOrder(Object userContext,long timeoutTime){
		if(readOrder!=null){
			return false;
		}
		readOrder=Order.create(context.getHandler(), OrderType.read, userContext);
		readOrder.setTimeoutTime(timeoutTime);
		selectOperator.asyncRead(readOrder);
		return true;
	}
	
	boolean writeOrder(Object userContext,ByteBuffer[] buffers,long asyncWriteStartOffset,long length,long timeoutTime){
		if( writeOperator.asyncWrite(buffers)==false){
			return false;
		}
		Order order=Order.create(context.getHandler(), OrderType.write, userContext);
		order.setWriteStartOffset(asyncWriteStartOffset);
		order.setWriteEndOffset(asyncWriteStartOffset+length);
		order.setTimeoutTime(timeoutTime);
		writeOrders.add(order);
		return true;
	}
	
	boolean closeOrder(Object userContext){
		if(closeOrder!=null){
			return false;
		}
		if( writeOperator.asyncClose()==false){
			return false;
		}
		closeOrder=Order.create(context.getHandler(), OrderType.close, userContext);
		return true;
	}
}
