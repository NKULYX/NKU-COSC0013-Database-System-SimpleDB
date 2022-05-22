package simpledb;

import java.util.Objects;

/**
 * @author TTATT
 * This class is an utils to describe the state of lock
 * It contains the following states:
 * TransactionId: which transaction is holding the lock
 * LockType: which type of lock is the transaction holding
 */
public class Lock {

    public static final String EXCLUSIVE_LOCK = "EXCLUSIVE_LOCK";
    public static final String SHARED_LOCK = "SHARED_LOCK";

    private TransactionId tid;
    private String lockType;

    public Lock(TransactionId tid, Permissions lockType) {
        this.tid = tid;
        // if the transaction's permission is READ_WRITE, then the lock is EXCLUSIVE_LOCK
        if(lockType.equals(Permissions.READ_WRITE)) {
            this.lockType = EXCLUSIVE_LOCK;
        }
        // if the transaction's permission is READ_ONLY, then the lock is SHARED_LOCK
        else {
            this.lockType = SHARED_LOCK;
        }
    }

    public TransactionId getTid() {
        return tid;
    }

    public String getLockType() {
        return lockType;
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj){
            return true;
        }
        if(obj==null||getClass()!=obj.getClass()){
            return false;
        }
        Lock objLock=(Lock) obj;
        return tid.equals(objLock.getTid())&&lockType.equals(objLock.getLockType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(tid, lockType);
    }
}
