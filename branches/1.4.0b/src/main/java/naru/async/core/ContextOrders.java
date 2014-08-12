package naru.async.core;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import naru.async.ChannelHandler;
import naru.async.core.ReadChannel.State;
import naru.async.pool.BuffersUtil;

import org.apache.log4j.Logger;
/**
 * 
 * ChannelContextが保持するOrderを管理
 * 排他は、ChannelContext側で獲る
 * @author naru
 *
 */
public class ContextOrders {
	private static Logger logger=Logger.getLogger(ContextOrders.class);
	
	private Order acceptOrder;
	private Order connectOrder;
	private Order readOrder;
	private LinkedList<Order> writeOrders=new LinkedList<Order>();
	private Order closeOrder;
	private Throwable failure;
	
	private ChannelContext context;

	public ContextOrders(ChannelContext context){
		this.context=context;
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
	
	/**
	 * dispatch workerから呼び出される、多重では走行しない
	 */
	public void callback(){
		Order order=null;
		boolean isFinishCallback=false;
		ChannelHandler finishHandler=null;
		//currentContext.set(this);
		try{
			while(true){
				synchronized(this){
					logger.debug("callback cid:"+context.getPoolId() +":size:"+callbackOrders.size());
					if(order!=null){
						callbackOrders.remove(order);
					}
					if(callbackOrders.size()==0){
						inCallback=false;
						break;
					}
					order=(Order)callbackOrders.get(0);
				}
				if(order.isFinish()){
					isFinishCallback=true;
					finishHandler=order.getHandler();
				}
				try{
					order.callback(stastics);
				}catch(Throwable t){
					logger.warn("callback return Throwable",t);
					//回線が切断されたとして処理
					//doneClosed(true);
				}
			}
		}finally{
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
	public void recycle() {
		acceptOrder=null;
		connectOrder=null;
		readOrder=null;
		writeOrders.clear();
		closeOrder=null;
		failure=null;
	}
	
	public boolean order(Order order){
		int type=order.getOrderType();
		
		if(failure!=null && type!=Order.TYPE_CLOSE){
			//誰にも通知していないエラーが溜まっていれば、通知
			logger.debug("aleady has failure."+type);
			order.setFailure(failure);
			queueCallback(order);
			failure=null;
			return true;
		}
		switch(type){
		case Order.TYPE_READ:
			if(readOrder!=null){
				logger.debug("aleady has readOrder."+readOrder,new Exception());
				return false;
			}
			readOrder=order;
			break;
		case Order.TYPE_WRITE:
			writeOrders.add(order);
			break;
		case Order.TYPE_SELECT:
			if(acceptOrder!=null){
				return false;
			}
			acceptOrder=order;
			break;
		case Order.TYPE_ACCEPT:
			//acceptは、selectの結果内部的に使われる、直接呼ばれる事はない
			break;
		case Order.TYPE_CONNECT:
			if(connectOrder!=null){
				return false;
			}
			if(context.isConnected()){
				//作ってすぐconnectしちゃった場合
				queueCallback(order);
			}else{
				connectOrder=order;
			}
			break;
		case Order.TYPE_CLOSE:
			if(closeOrder!=null){
				return false;
			}
			closeOrder=order;
			break;
		}
		return true;
	}
	
	public boolean isCloseable(){
		if(closeOrder==null){
			return false;
		}
		/*
		if(isReadOrder()){
			return false;
		}
		*/
		if(isWriteOrder()){
			return false;
		}
		if(connectOrder!=null){
			return false;
		}
		//acceptはなくならない、
		return true;
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
	
	/**
	 * ioLock synchronizedの中から呼び出す事
	 * @return
	 */
	public int operations(){
		if(connectOrder!=null){
			return SelectionKey.OP_CONNECT;
		}
		if(acceptOrder!=null){
			return SelectionKey.OP_ACCEPT;
		}
		int op=0;
		if(writeOrders.size()!=0){
			op|=SelectionKey.OP_WRITE;
		}
		if(readOrder!=null){
			op|=SelectionKey.OP_READ;
		}
		if(op==0){
			op=SelectionKey.OP_READ;
		}
//		logger.debug("operations:0x"+Integer.toHexString(op));
		return op;
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
	
	private static int FAILURE=2;
	private static int TIMEOUT=3;
	private static int CLOSE_ORDER=4;
	
	private int queueOrders(int event,int orderType,Throwable e){
		int count=0;
		if((orderType==Order.TYPE_NON||orderType==Order.TYPE_SELECT)&& acceptOrder!=null){
			if(event==FAILURE){
				acceptOrder.setFailure(e);
			}else if(event==TIMEOUT){
				//ありえない
			}else if(event==CLOSE_ORDER){
				acceptOrder.closeOrder();
			}
			Order order=acceptOrder;
			acceptOrder=null;
			queueCallback(order);
			count++;
		}
		if((orderType==Order.TYPE_NON||orderType==Order.TYPE_CONNECT)&&connectOrder!=null){
			if(event==FAILURE){
				connectOrder.setFailure(e);
			}else if(event==TIMEOUT){
				connectOrder.timeout();
			}else if(event==CLOSE_ORDER){
				connectOrder.closeOrder();
			}
			queueCallback(connectOrder);
			connectOrder=null;
			count++;
		}
		if((orderType==Order.TYPE_NON||orderType==Order.TYPE_READ)&&readOrder!=null){
			if(event==FAILURE){
				readOrder.setFailure(e);
			}else if(event==TIMEOUT){
				readOrder.timeout();
			}else if(event==CLOSE_ORDER){
				readOrder.closeOrder();
			}
			queueCallback(readOrder);
			readOrder=null;
			count++;
		}
		int writeCount=writeOrders.size();
		if((orderType==Order.TYPE_NON||orderType==Order.TYPE_WRITE)&&writeCount!=0){
			logger.debug("writeOrder collect callback.cid:"+context.getPoolId() +":writeCount:"+writeCount+":event:"+event);
			//TODO wirteBufferが取り残されてしまう
			Order writeOrder=writeOrders.get(0);
			Object[] userContexts=new Object[writeCount];
			for(int i=0;i<writeCount;i++){
				Order order=writeOrders.remove(0);
				userContexts[i]=order.getUserContext();
				if(i!=0){//複数write命令をまとめる処理
					order.unref(true);//通知したとみなす
				}
			}
			if(event==FAILURE){
				writeOrder.setFailure(e);
			}else if(event==TIMEOUT){
				writeOrder.timeout();
			}else if(event==CLOSE_ORDER){
				writeOrder.closeOrder();
			}
			writeOrder.setUserCountexts(userContexts);
			queueCallback(writeOrder);
			count+=writeCount;
		}
		if((orderType==Order.TYPE_NON||orderType==Order.TYPE_CLOSE)&&closeOrder!=null && event==CLOSE_ORDER){
			closeOrder.closeOrder();
			queueCallback(closeOrder);
			closeOrder=null;
			count++;
		}
		return count;
	}
	
	/**
	 * 全Orderにfailure通知
	 */
	public int failure(Throwable e){
		int count=queueOrders(FAILURE,Order.TYPE_NON,e);
		logger.debug("failure.cid:"+context.getPoolId()+":count:"+count);
		if(count==0){
			//誰にも通知しなかったら採っておいて次のリクエストに通知する。
			failure=e;
		}
		return count;
	}
	
	/**
	 * 指定されたorderにtimeout通知
	 */
	public int timeout(int orderType){
		return queueOrders(TIMEOUT,orderType,null);
	}
	
	/**
	 * 指定されたorderにclosed通知
	 */
	public int closed(Order finishOrder){
		int count=queueOrders(CLOSE_ORDER,Order.TYPE_NON,null);
		queueCallback(finishOrder);
		return count+1;
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
	
	public boolean doneDisconnect(){
		if(readOrder==null){
			return false;
		}
		Order order=readOrder;
		readOrder=null;
		order.setOrderType(Order.TYPE_CLOSE);
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
		acceptOrder=Order.create(context.getHandler(), Order.TYPE_SELECT, userContext);
		context.getReadChannel().queueSelect(State.accepting);
		return true;
	}
	
	boolean connectOrder(Object userContext){
		if(connectOrder!=null){
			return false;
		}
		connectOrder=Order.create(context.getHandler(), Order.TYPE_CONNECT, userContext);
		if(context.isConnected()){//すでにconnectが成功していた場合
			queueCallback(connectOrder);
			connectOrder=null;
		}
		context.getReadChannel().queueSelect(State.reading);
		return true;
	}
	
	boolean readOrder(Object userContext){
		if(readOrder!=null){
			return false;
		}
		readOrder=Order.create(context.getHandler(), Order.TYPE_READ, userContext);
		if( context.getReadChannel().asyncRead(readOrder) ){
			readOrder=null;
		}
		return true;
	}
	
	boolean writeOrder(Object userContext,ByteBuffer[] buffers,long asyncWriteStartOffset,long length){
		if( context.getWriteChannel().asyncWrite(buffers)==false){
			return false;
		}
		Order order=Order.create(context.getHandler(), Order.TYPE_WRITE, userContext);
		order.setWriteStartOffset(asyncWriteStartOffset);
		order.setWriteEndOffset(asyncWriteStartOffset+length);
		writeOrders.add(order);
		return true;
	}
	
	boolean closeOrder(Object userContext){
		if(closeOrder!=null){
			return false;
		}
		if( context.getWriteChannel().asyncClose()==false){
			return false;
		}
		closeOrder=Order.create(context.getHandler(), Order.TYPE_CLOSE, userContext);
		return true;
	}

}
