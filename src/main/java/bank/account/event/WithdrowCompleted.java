package bank.account.event;

/**
 * Created by dmalalan on 11/10/16.
 */
public class WithdrowCompleted implements Event{
    public int value;

    public WithdrowCompleted(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "WithdrowCompleted{" +
                "value=" + value +
                '}';
    }
}
