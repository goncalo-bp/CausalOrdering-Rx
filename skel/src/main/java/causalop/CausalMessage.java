package causalop;

import java.util.HashMap;
import java.util.Map;

public class CausalMessage<T> {
    int j;
    Map<Integer,Integer> v;
    public T payload;

    public CausalMessage(T payload, int j, Map<Integer, Integer> v) {
        this.payload = payload;
        this.j = j;
        this.v = new HashMap<>(v);
    }
}
