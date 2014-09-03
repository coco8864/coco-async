package naru.async;

import org.apache.log4j.Logger;

public class Log{
	public static void debug(Logger logger,Object...args){
		if(!logger.isDebugEnabled()){
			return;
		}
		Throwable t=null;
		StringBuilder message=new StringBuilder();
		for(Object arg:args){
			if(arg instanceof Throwable){
				t=(Throwable)arg;
				break;
			}
			message.append(arg);
		}
		if(t!=null){
			logger.debug(message,t);
		}else{
			logger.debug(message);
		}
	}
}
