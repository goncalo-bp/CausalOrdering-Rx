package causalop;

import io.reactivex.rxjava3.core.Observable;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class CausalOpTest {
    @Test
    public void testOk() {
        var l = Observable.just(
                        new CausalMessage<String>("a", 1, new HashMap<Integer, Integer>() {{ put(1, 1); }}), // (0,1)
                        new CausalMessage<String>("b", 0, new HashMap<Integer, Integer>() {{ put(0, 1); }}), // (1,0)
                        new CausalMessage<String>("c", 1, new HashMap<Integer, Integer>() {{ put(1, 2); }}), // (1,2)
                        new CausalMessage<String>("e", 1, new HashMap<Integer, Integer>() {{ put(0,2); put(1, 3); }}), // (2,3)
                        new CausalMessage<String>("d", 0, new HashMap<Integer, Integer>() {{ put(0,2); }}) // (2,2)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c","d","e"});
    }

    @Test
    public void testReorder() {
        var l = Observable.just(
                        new CausalMessage<String>("c", 1, new HashMap<Integer, Integer>() {{ put(0,1); put(1,2); }}), // (1,2)
                        new CausalMessage<String>("a", 1, new HashMap<Integer, Integer>() {{ put(1,1); }}), // (0,1)
                        new CausalMessage<String>("b", 0, new HashMap<Integer, Integer>() {{ put(0,1); }}) // (1,0)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testDupl() {
        var l = Observable.just(
                        new CausalMessage<String>("a", 1, new HashMap<Integer, Integer>() {{ put(1,1); }}), // (0,1)
                        new CausalMessage<String>("b", 0, new HashMap<Integer, Integer>() {{ put(0,1); }}), // (1,0)
                        new CausalMessage<String>("a", 1, new HashMap<Integer, Integer>() {{ put(1,1); }}), // (0,1)
                        new CausalMessage<String>("c", 1, new HashMap<Integer, Integer>() {{ put(1,2); }}) // (1,2)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGap() {
        var l = Observable.just(
                        new CausalMessage<String>("c", 1, new HashMap<Integer, Integer>() {{ put(0,1); put(1,2); }}), // (1,2)
                        new CausalMessage<String>("a", 1, new HashMap<Integer, Integer>() {{ put(1,1); }}) // (0,1)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();
    }
}
