package wiki.xyh.process;

import org.apache.flink.api.common.functions.FilterFunction;

public class NonNullFilter implements FilterFunction<String> {

    @Override
    public boolean filter(String value) {
        return value != null;
    }
}
