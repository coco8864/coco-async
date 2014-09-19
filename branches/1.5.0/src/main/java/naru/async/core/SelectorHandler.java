package naru.async.core;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;

import naru.async.Log;
import naru.async.core.SelectOperator.State;
import naru.async.pool.LocalPoolManager;

public class SelectorHandler implements Runnable {
	static private Logger logger=Logger.getLogger(SelectorHandler.class);
	private int id;
	
	private long selectInterval;
	private SelectorStastics stastics;
	private Selector selector;
	
	private Thread selectorThread;
	private LinkedList<ChannelContext> contexts=new LinkedList<ChannelContext>();
	private boolean isWakeup;
	private boolean isRun;
	
	public int getId(){
		return id;
	}
	SelectorHandler(int id,long selectInterval) throws IOException{
		this.id=id;
		this.selectInterval=selectInterval;
		this.selector = Selector.open();
		this.stastics=new SelectorStastics(id,selectInterval);
	}
	
	SelectorStastics getStastics(){
		return stastics;
	}
	
	private static class AcceptRunner implements Runnable{
		private SelectorHandler handler;
		private ChannelContext context;
		AcceptRunner(SelectorHandler handler,ChannelContext context){
			this.handler=handler;
			this.context=context;
		}
		public void run() {
			logger.info("accept thread start");
			LocalPoolManager.setupChargeClassPool(Order.class,1024);
			handler.accept(context);
			logger.info("accept thread end");
			context.unref();
		}
		private void start(){
			ServerSocketChannel serverSocketChannel=(ServerSocketChannel)context.getChannel();
			try {
				serverSocketChannel.configureBlocking(true);
			} catch (IOException e) {
			}
			Thread thread=new Thread(this);
			try {
				thread.setName("accept-"+serverSocketChannel.getLocalAddress());
			} catch (IOException e) {
				logger.warn("getLocalAddress error",e);
			}
			thread.start();
		}
	}
	
	public void queueSelect(ChannelContext context){
		Log.debug(logger,"queueSelect.cid:",context.getPoolId());
		stastics.inQueue();
		synchronized(contexts){
			context.ref();//select cycleにいる間は参照を持つ
			if(context.getSelectOperator().isAccepting()&&IOManager.isAcceptThread()){
				(new AcceptRunner(this,context)).start();
			}else{
				contexts.add(context);
			}
		}
		wakeup();
	}
	public void wakeup(){
		if(isWakeup){
			return;
		}
		//複数スレッドから同時に呼び出された場合、selector.wakeup()が複数呼び出される
		//可能性があるが容認（synchronizedしてまで守る必要なし)
		isWakeup=true;
		selector.wakeup();
	}
	
	public SelectionKey keyFor(SelectableChannel channel){
		return channel.keyFor(selector);
	}
	
	public  SelectionKey register(SelectableChannel channel,int ops,Object att) throws ClosedChannelException{
		return channel.register(selector, ops, att);
	}
	
	/**
	 * 
	 * @param nextWakeup 次のタイムアウト時刻で一番近い時刻
	 * @return
	 */
	private void closeAll(){
		Iterator itr=selector.keys().iterator();
		while(itr.hasNext()){
			SelectionKey key=(SelectionKey)itr.next();
			if(key.isValid()==false){
				continue;
			}
			ChannelContext context=(ChannelContext)key.attachment();
			context.getSelectOperator().queueIo(State.closing);
			context.cancelSelect();
		}
	}
	
	private long checkIn(long timeoutTime,Set<ChannelContext> selectIn){
		//Set<ChannelContext> nextContexts=new HashSet<ChannelContext>();
		while(true){
			ChannelContext context;
			synchronized(contexts){
				if(contexts.size()==0){
					break;
				}
				context=(ChannelContext)contexts.remove(0);
			}
			if(context.select()){
				long time=context.getNextSelectWakeUp();
				if(time<timeoutTime){
					timeoutTime=time;
				}
				selectIn.add(context);
			}else{//参加したかったが、参加させられなかった。
				Log.debug(logger,"selectAdd fail to add.cid:",context.getPoolId());
				//nextContexts.add(context);
			}
		}
		//for(ChannelContext context:nextContexts){
		//	queueSelect(context);
		//}
		return timeoutTime;
	}

	private void accept(ChannelContext context){
		while(acceptInternal(context)){
		}
	}

	private boolean acceptInternal(ChannelContext context){
		ServerSocketChannel serverSocketChannel=(ServerSocketChannel)context.getChannel();
		SocketChannel socketChannel;
		try {
			socketChannel = serverSocketChannel.accept();
			if(socketChannel==null){
				return false;
			}
			if( !context.acceptable(socketChannel) ){
				Log.debug(logger,"refuse socketChannel:",socketChannel);
				socketChannel.close();//接続拒否
				stastics.acceptRefuse();
				return true;
			}
			Log.debug(logger,"acceptInternal:",socketChannel);
		} catch (IOException e) {
			logger.error("fail to accept.",e);
			return false;
		}
		return true;
	}
	
	private boolean dispatch(SelectionKey key,ChannelContext context){
		if(key.isWritable()){
			stastics.write();
			context.getWriteOperator().writable();
		}
		if(key.isConnectable()){
			stastics.connect();
			context.getSelectOperator().connectable();
			return true;
		}else if(key.isReadable()){
			stastics.read();
			context.getSelectOperator().readable();
			return true;
		}else if(key.isAcceptable()){
			accept(context);
		}
		return false;
	}
	
	private long checkOutContext(Set<SelectionKey> selectedKeys,SelectionKey key,ChannelContext context,long timeoutTime,Set<ChannelContext> selectOut){
		if(!key.isValid()||!context.isSelectionKey()){
			selectOut.add(context);
			return timeoutTime;
		}else if(selectedKeys.contains(key)){
			Log.debug(logger,"checkOutContext selectedKeys cid:",context.getPoolId());
			if(dispatch(key, context)){
				//dispatchに成功したなら、selectを続ける必要なし
				selectOut.add(context);
				return timeoutTime;
			}
		}
		if(context.select()){
			Log.debug(logger,"checkOutContext context.select() cid:",context.getPoolId());
			long time=context.getNextSelectWakeUp();
			if(time<timeoutTime){
				timeoutTime=time;
			}
			Log.debug(logger,"after select getNextSelectWakeUp.cid:",context.getPoolId(),":",(timeoutTime-System.currentTimeMillis()));
		}else{//参加させられなかった。
			Log.debug(logger,"checkOutContext fail to add.cid:",context.getPoolId());
			selectOut.add(context);
		}
		return timeoutTime;
	}
	
	private long checkOut(long timeoutTime,Set<ChannelContext> selectOut){
		Set<SelectionKey> keys=selector.keys();
		Iterator<SelectionKey> itr=keys.iterator();
		Set<SelectionKey> selectedKeys=selector.selectedKeys();
		while(itr.hasNext()){
			SelectionKey key=itr.next();
			ChannelContext context=(ChannelContext)key.attachment();
			Log.debug(logger,"checkOut cid:",context.getPoolId());
			synchronized(context){
				timeoutTime=checkOutContext(selectedKeys,key,context, timeoutTime, selectOut);
			}
		}
		return timeoutTime;
	}

	private static long MIN_INTERVAL_TIME=10;
	private static int MIN_INTERVAL_COUNT=16;
	/**
	 * リクエスト待ち、このメソッドはブロックします。
	 * リクエスト到着は、ChannelEventに通知します。
	 * 
	 * @throws IOException
	 */
	private void waitForSelect() throws IOException {
		isRun=true;
		isWakeup=false;//若干余分にwakeupが呼び出されるのは許容する
		long nextWakeup=System.currentTimeMillis()+selectInterval;//次にtimeoutする直近時刻
		long lastWakeup=System.currentTimeMillis();
		int minCount=0;
		Set<ChannelContext> selectOut=new HashSet<ChannelContext>();
		Set<ChannelContext> selectIn=new HashSet<ChannelContext>();
		while (true) {
			long now=System.currentTimeMillis();
			if( (lastWakeup-now)<MIN_INTERVAL_TIME ){
				minCount++;
//				System.out.println("minCount:"+minCount);
				if(minCount>=MIN_INTERVAL_COUNT){
					try {
						stastics.sleep();
						Thread.sleep(MIN_INTERVAL_TIME);
					} catch (InterruptedException ignore) {
					}
					now=System.currentTimeMillis();
					minCount=0;
				}
			}else{
				minCount=0;
			}
			long interval=nextWakeup-now;
			if(interval>selectInterval){
				interval=selectInterval;
			}
			Log.debug(logger,"select in.interval:",interval);
			stastics.loop();
			int selectCount=0;
			lastWakeup=System.currentTimeMillis();
			Set<SelectionKey> keys=selector.keys();
			stastics.setSelectCount(keys.size());
			if(interval>0){
				selectCount=selector.select(interval);
			}else{
				selectCount=selector.selectNow();
			}
			isWakeup=false;//排他してないので、若干余分にwakeupが呼び出されるのは許容する
			if(selectCount<0){
				logger.info("select out break.selectCount:"+selectCount+":this:"+this);
				break;
			}else if(selectCount!=0){
				minCount=0;//イベントが発生してwakeupされたならカウンタをクリア
			}
			/* 選択されたチャネルを処理する */
			nextWakeup=System.currentTimeMillis()+selectInterval;
			nextWakeup=checkOut(nextWakeup,selectOut);
			nextWakeup=checkIn(nextWakeup,selectIn);
			/* selectOutの後、selectInしていないkeyをcancelする */
			Iterator<ChannelContext> outItr=selectOut.iterator();
			while(outItr.hasNext()){
				ChannelContext out=outItr.next();
				if(!selectIn.contains(out)){
					out.cancelSelect();
					out.unref();
				}
				outItr.remove();
			}
			selectIn.clear();
			Log.debug(logger,"nextWakeup:",nextWakeup,":",(nextWakeup-System.currentTimeMillis()));
			
			/* 停止要求されている場合は、停止 */
			if( isRun==false ){
				closeAll();
				selector.close();
				break;
			}
		}
	}
	
	public void run() {
		try {
			waitForSelect();
			logger.info("SelectorContext nomal end.stastics:"+stastics.getLoopCount() +":"+ stastics.getSleepCount());
		} catch (IOException e) {
			logger.error("SelectorContext listener IOException end.",e);
		} catch (Throwable e) {
			logger.error("SelectorContext listener Throwable end.",e);
		}
	}
	
	public void start(int index){
		isRun=true;
		selectorThread=new Thread(this);
		selectorThread.setName("selector-" + index);
		selectorThread.start();
	}
	
	public void stop(){
		isRun=false;
		selector.wakeup();
		try {
			selectorThread.join();
		} catch (InterruptedException e) {
			logger.error("fail to join",e);
		}
	}
}
