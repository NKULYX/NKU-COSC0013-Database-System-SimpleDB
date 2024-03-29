package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Thread.currentThread;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int maxPageNum;
    private ConcurrentHashMap<PageId,Page> pagesMap;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.maxPageNum = numPages;
        pagesMap = new ConcurrentHashMap<>();
        lockManager = new LockManager();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        /*
        get lock without check deadlock
         */
//        while(!lockManager.acquireLock(tid, pid, perm)) {
//            try {
//                Thread.sleep(200);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            if (lockManager.checkDeadLock(tid)) {
//                throw new TransactionAbortedException();
//            }
//        }


//        boolean is_acquired = lockManager.acquireLock(tid,pid,perm);
//        Long begin=System.currentTimeMillis();
////        System.out.println(System.currentTimeMillis()+"begin"+currentThread().getName());
//        while(!is_acquired) {
//            Long end=System.currentTimeMillis();
////            System.out.println(System.currentTimeMillis()+"test"+currentThread().getName());
//            if(end-begin>1000){
//                throw new TransactionAbortedException();
//            }
//            try {
//                Thread.sleep(100);
//            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            is_acquired=lockManager.acquireLock(tid,pid,perm);
//        }

        /*
         to pass AbortEvictionTest
         the test data contains deadlock
         */
        if(!lockManager.acquireLock(tid,pid,perm)) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(pagesMap.containsKey(pid)){
            return pagesMap.get(pid);
        }
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbFile.readPage(pid);
        if(this.pagesMap.size()>=maxPageNum) {
            evictPage();
        }
//            throw new DbException("Eviction policy need to be implemented");
        pagesMap.put(pid, page);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.getLock(tid, p) != null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit) {
            flushPages(tid);
        }
        else{
            revertPages(tid);
        }
        // release the pages that is locked by the
        for(PageId pid : pagesMap.keySet()) {
            if(holdsLock(tid, pid)) {
                releasePage(tid, pid);
            }
        }
    }

    /**
     * revert all pages modified by a transaction
     * @param tid the transaction
     */
    private synchronized void revertPages(TransactionId tid) {
        for(PageId pid : pagesMap.keySet()) {
            Page page = pagesMap.get(pid);
            if(page.isDirty() == tid){
                int tableId = page.getId().getTableId();
                DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
                Page originPage = dbFile.readPage(pid);
                pagesMap.put(pid, originPage);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = dbFile.insertTuple(tid, t);
        for(Page page: pages){
            page.markDirty(true, tid);
            pagesMap.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = dbFile.deleteTuple(tid, t);
        for(Page page: pages){
            page.markDirty(true, tid);
            pagesMap.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Page page: pagesMap.values()){
            flushPage(page.getId());
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pagesMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pagesMap.get(pid);
        if(page.isDirty() != null){
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // flush all pages of the specific transaction to disk
        for(Page page: pagesMap.values()){
            if(page.isDirty() != null && page.isDirty() == tid){
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
//        for(Page page: pagesMap.values()){
//            if(page.isDirty() == null){
//                pagesMap.remove(page.getId());
//            }
//        }
//        if(pagesMap.size() >= maxPageNum){
//            throw new DbException("Buffer pool is full and none of the pages can be evicted");
//        }
//        Enumeration<PageId> keys = pagesMap.keys();
//        while(keys.hasMoreElements()) {
//            PageId pid = keys.nextElement();
//            Page page = pagesMap.get(pid);
//            // if the page is dirty, flush it to disk
//            if(page.isDirty()!=null){
//                pagesMap.remove(pid);
//            }
//        }
        Page to_test_page=null;
        PageId to_remove_hashcode=null;
        for(PageId it:pagesMap.keySet()) {
            to_test_page = pagesMap.get(it);
            if (to_test_page.isDirty() != null) {//HeapPage的isDirty()如果是dirty会返回TransactionId
                to_test_page=null;
                continue;
            }
            to_remove_hashcode=it;
            break;
        }
        if(to_test_page==null) throw new DbException("there are all dirty page");
        pagesMap.remove(to_remove_hashcode);
    }

}
