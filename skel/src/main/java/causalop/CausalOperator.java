package causalop;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.ObservableOperator;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.observers.DisposableObserver;

import java.util.*;

public class CausalOperator<T> implements ObservableOperator<T, CausalMessage<T>> {
    private final int n;

    private List<CausalMessage<T>> queue;
    private int[] seqNums;

    public CausalOperator(int n) {
        this.n = n;
        this.seqNums = new int[n];
        Arrays.fill(this.seqNums, 0);
        this.queue = new ArrayList<>();
    }

    private Boolean canDeliver(int j, int[] v) {
        if (v[j] != seqNums[j] + 1)
            return false;
        for (int i = 0; i < n; i++)
            if (i != j && v[i] > seqNums[i])
                return false;
        return true;
    }

    private Boolean checkDup(CausalMessage<T> m) {
        for (CausalMessage<T> entry : queue)
            if (entry.j == m.j && entry.v[m.j] == m.v[m.j])
                return true;
        return false;
    }

    private CausalMessage<T> checkQueue() throws Exception{
        for (CausalMessage<T> entry : queue) {
            if (canDeliver(entry.j, entry.v))
                return entry;
        }
        throw new Exception("Can't deliver");
    }

    @Override
    public @NonNull Observer<? super CausalMessage<T>> apply(@NonNull Observer<? super T> down) throws Throwable {
        return new DisposableObserver<>() {
            @Override
            public void onNext(@NonNull CausalMessage<T> m) {
                if (canDeliver(m.j, m.v)) {
                    // Deliver message
                    down.onNext(m.payload);
                    seqNums[m.j]++;

                    // Retest queue
                    while (true) {
                        try {
                            CausalMessage<T> msg = checkQueue();
                            // Deliver message
                            down.onNext(msg.payload); // Entrega
                            seqNums[msg.j]++;         // Incrementa número de sequência
                            queue.remove(msg);        // Remove da queue
                        } catch (Exception e) {
                            break;
                        }
                    }
                }
                else if (m.v[m.j] > seqNums[m.j] && !checkDup(m)) queue.add(m);
            }

            @Override
            public void onError(@NonNull Throwable e) {
                down.onError(e); // FIXME
            }

            @Override
            public void onComplete() {
                if (!queue.isEmpty())
                    onError(new IllegalArgumentException("gap detected"));
                down.onComplete();
            }
        };
    }
}
