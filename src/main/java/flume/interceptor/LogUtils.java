package flume.interceptor;


import org.apache.commons.lang.math.NumberUtils;

/**
 * @program:flume-interceptor
 * @package:flume.interceptor
 * @filename:LogUtils.java
 * @create:2019.10.30.11.28
 * @author:Administrator
 * @descrption.
 */
public class LogUtils {

    public static boolean valuateStart(String log) {
        //1启动日志json
        if (log==null)
            return false;
        if (!log.trim().startsWith("{ ")||!log.trim().endsWith("}"))
            return false;
        return true;
    }

    public static boolean valuateEvents(String log) {
        if (log==null)
            return false;
        String[] logContext = log.split("\\|");

        if (logContext.length!=2)
            return false;
        if (logContext[0].length()!=13||!NumberUtils.isDigits(logContext[0]))
            return false;
        if (!logContext[1].trim().startsWith("{ ")||!logContext[1].trim().endsWith("}"))
            return false;
        return true;
    }
}
