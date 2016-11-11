package bank.account;

import bank.account.event.AccountOpened;
import bank.account.event.Event;
import bank.account.event.WithdrowCompleted;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dmalalan on 11/10/16.
 */
public class BankAccount {
    private int value;
    private int accountID;

    private List<Event> eventList = new ArrayList<Event>();

    BankAccount(int accoutID) {
        this.accountID = accoutID;
        this.eventList.add(new AccountOpened(accoutID));
    }

    public void withdraw(int amount) {
        this.value = this.value - amount;
        this.eventList.add(new WithdrowCompleted(amount));
    }

    public void deposit(int amount) {
        this.value = this.value + amount;
    }

    public void construct(){
        for (Event e:eventList) {
        }
    }

    public void Apply(AccountOpened e){
        this.accountID = e.accountID;
    }

    public void Apply(WithdrowCompleted e){
        this.value = this.value - e.value;
    }
}
