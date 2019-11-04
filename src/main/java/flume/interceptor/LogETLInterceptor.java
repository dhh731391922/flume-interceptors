package flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @program:flume-interceptor
 * @package:flume.interceptor
 * @filename:LogETLInterceptor.java
 * @create:2019.10.30.10.32
 * @author:Administrator
 * @descrption.
 */
public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1.获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));
        //2.校验  启动日志（json）  事件日志（服务器时间|json）
        if (log.contains("start")){
            //检验启动日志
            if (LogUtils.valuateStart(log)){
                return event;
            }
        }else {
            if(LogUtils.valuateEvents(log)){
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> interceptors = new ArrayList<>();
        for (Event event:list){
            Event intercept1 = intercept(event);
            if (intercept1!=null)
                interceptors.add(intercept1);
        }
        return interceptors;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
