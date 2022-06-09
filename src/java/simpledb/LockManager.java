package simpledb;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author TTATT
 * This class is an util to manage the locks
 */
public class LockManager {

    /*
    dependenciesSet stores the dependencies that associated with current transaction
    pageLocks stores the locks that a page holds
    using ConcurrentHashMap because it is thread-safe
     */
    private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> dependenciesSet;
    private ConcurrentHashMap<PageId,LinkedList<Lock>> pageLocks;

    public LockManager() {
        dependenciesSet = new ConcurrentHashMap<TransactionId, HashSet<TransactionId>>();
        pageLocks = new ConcurrentHashMap<PageId, LinkedList<Lock>>();
    }

    /**
     * a specific transaction acquires the lock on the specific page
     * @param tid transaction id
     * @param pid page id
     * @param permissions the permission of the transaction on the page
     */
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions permissions){
        // if the permission is READ_ONLY, then acquire the SHARED_LOCK
        if(permissions.equals(Permissions.READ_ONLY)){
            return acquireSharedLock(tid, pid);
        }
        // else the permission is READ_WRITE, then acquire the EXCLUSIVE_LOCK
        else{
            return acquireExclusiveLock(tid, pid);
        }
    }

    /**
     * a specific transaction acquires the SHARED_LOCK on the specific page
     * @param tid transaction id
     * @param pid page id
     * @return true if the transaction acquires the lock successfully
     */
    private synchronized boolean acquireSharedLock(TransactionId tid, PageId pid) {
        // get the locks on the target page
        LinkedList<Lock> lockList = pageLocks.get(pid);
        // check if the locks on the target page is null, then create a new locks list
        if(lockList==null){
            lockList = new LinkedList<Lock>();
        }
        if(lockList.size() > 0) {
            /*
            There are 3 cases
            1. there is no EXCLUSIVE_LOCK in the list
                can get the lock successfully
            2. there is an EXCLUSIVE_LOCK in the list and the lock belongs to the transaction
                can the lock successfully
            3. there is an EXCLUSIVE_LOCK in the list and the lock doesn't belong to the transaction
                can not get the lcok
             */
            for (Lock lock : lockList) {
                // check if there is an EXCLUSIVE_LOCK
                if (lock.getLockType().equals(Lock.EXCLUSIVE_LOCK) && !lock.getTid().equals(tid)) {
                    // if the EXCLUSIVE_LOCK belongs to other transaction
                    addDependency(tid, lock.getTid());
                    return false;
                }
                // the lock is a SHARED_LOCK
                else {
                    // if the SHARED_LOCK belongs to the target transaction
                    if (lock.getTid().equals(tid)) {
                        return true;
                    }
                }
            }
            // there is no lock belongs to the target transaction
            addLock(tid, pid, Permissions.READ_ONLY);
            return true;
        }
        else{
            addLock(tid, pid, Permissions.READ_WRITE);
            return true;
        }
    }

    /**
     * a specific transaction acquires the EXCLUSIVE_LOCK on the specific page
     * @param tid transaction id
     * @param pid page id
     * @return true if the transaction acquires the lock successfully
     */
    private synchronized boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
        // get the locks on the target page
        LinkedList<Lock> lockList = pageLocks.get(pid);
        // check if the locks on the target page is null, then create a new locks list
        if(lockList==null){
            lockList = new LinkedList<Lock>();
        }
        if(lockList.size() > 0){
             /*
            There are  cases
            1. there is no EXCLUSIVE_LOCK in the list or there is an EXCLUSIVE_LOCK belongs to other transaction
                can not get the lock and add dependency to all the transactions
            2. there is an EXCLUSIVE_LOCK in the list and the lock belongs to the transaction
                can get the lock
             */
            for (Lock lock : lockList) {
                // check if there is an EXCLUSIVE_LOCK
                if(!lock.getTid().equals(tid)){
                    addDependency(tid, lock.getTid());
                }
                if (lock.getLockType().equals(Lock.EXCLUSIVE_LOCK)) {
                    // if the EXCLUSIVE_LOCK belongs to other transaction
                    if(!lock.getTid().equals(tid)){
                        return false;
                    }
                    else{
                        return true;
                    }
                }
            }
            return false;
        }
        else{
            addLock(tid,pid, Permissions.READ_WRITE);
            return true;
        }
    }

    /**
     * add lock to the lock list of the specific page
     * @param tid transaction id
     * @param pid page id
     * @param permission permission of the lock
     */
    private synchronized void addLock(TransactionId tid, PageId pid, Permissions permission) {
        Lock lock = new Lock(tid, permission);
        LinkedList<Lock> lockList = pageLocks.get(pid);
        if(lockList==null){
            lockList = new LinkedList<Lock>();
        }
        lockList.add(lock);
        pageLocks.put(pid, lockList);
        // when the lock can be added, it means that the transaction has no dependency
        deleteDependency(tid);
    }

    /**
     * add dependency to the transaction
     * @param tid transaction id
     * @param dependedTid transaction id that the transaction depends on
     */
    private synchronized void addDependency(TransactionId tid, TransactionId dependedTid) {
        if(tid.equals(dependedTid)){
            return;
        }
        HashSet<TransactionId> transactionIds = dependenciesSet.get(tid);
        if(transactionIds==null){
            transactionIds = new HashSet<TransactionId>();
        }
        transactionIds.add(dependedTid);
        dependenciesSet.put(tid, transactionIds);
    }

    /**
     * delete the dependency of the transaction
     * @param tid transaction id
     */
    private synchronized void deleteDependency(TransactionId tid) {
        dependenciesSet.remove(tid);
    }

    /**
     * release the lock on the specific page that belongs to the transaction
     * @param tid transaction id
     * @param pid page id
     */
    public synchronized void releasePage(TransactionId tid, PageId pid){
        LinkedList<Lock> lockList = pageLocks.get(pid);
        if(lockList==null || lockList.size() == 0){
            return;
        }
        for(Lock lock : lockList){
            if(lock.getTid().equals(tid)){
                lockList.remove(lock);
                break;
            }
        }
        if(lockList.size() == 0){
            pageLocks.remove(pid);
        }
    }

    /**
     * get the lock of the specific transaction on the specific page
     * @param tid transaction id
     * @param pid page id
     * @return
     */
    public synchronized Lock getLock(TransactionId tid, PageId pid){
        LinkedList<Lock> lockList = pageLocks.get(pid);
        if(lockList==null || lockList.size() == 0){
            return null;
        }
        for(Lock lock : lockList){
            if(lock.getTid().equals(tid)){
                return lock;
            }
        }
        return null;
    }

    public synchronized boolean checkDeadLock(TransactionId tid){
        Set<TransactionId> diverseid=new HashSet<>();
        Queue<TransactionId> que=new ConcurrentLinkedQueue<>();
        que.add(tid);

        while(que.size()>0){
            TransactionId remove_tid=que.remove();
            if(diverseid.contains(remove_tid)) {
                continue;
            }
            diverseid.add(remove_tid);
            Set<TransactionId> now_set=dependenciesSet.get(remove_tid);
            if(now_set==null) {
                continue;
            }
            for(TransactionId now_tid:now_set){
                que.add(now_tid);
            }
        }

        ConcurrentHashMap<TransactionId,Integer> now_rudu=new ConcurrentHashMap<>();
        for(TransactionId now_tid:diverseid){
            now_rudu.put(now_tid,0);
        }
        for(TransactionId now_tid:diverseid){
            Set<TransactionId> now_set=dependenciesSet.get(now_tid);
            if(now_set==null) {
                continue;
            }
            for(TransactionId now2_tid:now_set){
                Integer temp = now_rudu.get(now2_tid);
                temp++;
                now_rudu.put(now2_tid,temp);
            }
        }

        while(true){
            int cnt=0;
            for(TransactionId now_tid:diverseid){
                if(now_rudu.get(now_tid)==null) {
                    continue;
                }
                if(now_rudu.get(now_tid)==0){
                    Set<TransactionId> now_set=dependenciesSet.get(now_tid);
                    if(now_set==null) {
                        continue;
                    }
                    for(TransactionId now2_tid:now_set){
                        Integer temp = now_rudu.get(now2_tid);
                        if(temp==null) {
                            continue;
                        }
                        temp--;
                        now_rudu.put(now2_tid,temp);
                    }
                    now_rudu.remove(now_tid);
                    cnt++;
                }
            }
            if(cnt==0) {
                break;
            }
        }

        if(now_rudu.size()==0) {
            return false;
        }
        return true;
    }

}
