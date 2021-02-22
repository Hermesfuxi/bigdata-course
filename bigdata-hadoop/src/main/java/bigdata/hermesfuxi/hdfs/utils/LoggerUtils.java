package bigdata.hermesfuxi.hdfs.utils;

import org.apache.log4j.Logger;


public class LoggerUtils {
    private static final Logger logger = Logger.getLogger(LoggerUtils.class);

    public static void info(Object message){
        logger.info(message);
    }

    public static void debug(Object message){
        logger.debug(message);
    }
    public static void error(Object message){
        logger.error(message);
    }
    public static void fatal(Object message){
        logger.fatal(message);
    }

    public static void trace(Object message){
        logger.trace(message);
    }
}
