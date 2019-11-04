package flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @program:flume-interceptor
 * @package:flume.interceptor
 * @filename:LogTypeInterceptor.java
 * @create:2019.10.30.16.06
 * @author:Administrator
 * @descrption.类型区分过滤器
 */
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));
        Map<String, String> headers = event.getHeaders();
        if (log.contains("start")){
            headers.put("topic","topic_start");
        }else {
            headers.put("topic","topic_event");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> interceptors = new ArrayList<>();
        for (Event event : list) {
            Event intercept1 = intercept(event);
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
