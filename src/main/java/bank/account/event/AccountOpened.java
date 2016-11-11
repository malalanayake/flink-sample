package bank.account.event;

import bank.account.BankAccount;

/**
 * Created by dmalalan on 11/10/16.
 */
public class AccountOpened implements Event {
    public int accountID;

    public AccountOpened(int accountID){
        this.accountID = accountID;
    }

}
