package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    private int numPage;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
        this.numPage = (int) (this.file.length()/BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // some code goes here
        byte[] data = new byte[BufferPool.getPageSize()];
        HeapPage page = null;
        RandomAccessFile raf = null;
        /**
         * Get the file using RandomAccessFile
         * in order to use seek to jump to the beginning of this page
         */
        try {
            raf = new RandomAccessFile(this.file,"r");
            raf.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            raf.read(data);
            page = new HeapPage((HeapPageId) pid, data);
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
            byte[] data = page.getPageData();
            raf.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.numPage;
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<Page>();
        // first traverse through the pages to find if any page the tuple can be inserted into
        for(int i = 0; i < this.numPages(); i++) {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() != 0) {
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                pages.add(heapPage);
                break;
            }
        }
        // if no page can be inserted into, create a new page and insert the tuple into it
        if(pages.size() == 0) {
            // create a new page
            HeapPageId pid = new HeapPageId(this.getId(), numPage);
            HeapPage newPage = new HeapPage(pid, HeapPage.createEmptyPageData());
            // write the page to the disk
            writePage(newPage);
            this.numPage++;
            // read the page from the buffer pool
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            // insert the tuple into the new page
            heapPage.insertTuple(t);
            heapPage.markDirty(true, tid);
            pages.add(heapPage);
        }
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<Page>();
        // first find if any page contains the tuple to be deleted
        PageId pid = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        if(heapPage == null) {
            throw new DbException("Tuple not found in this table");
        }
        heapPage.deleteTuple(t);
        heapPage.markDirty(true, tid);
        pages.add(heapPage);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            int currentPageIndex;
            HeapPage currentPage;
            Iterator<Tuple> tupleIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.currentPageIndex = 0;
                /*
                  Get the first HeapPage in this DbFile
                  Must get the page though the BufferPool
                  in order to add the page into the BufferPool
                 */
                this.currentPage = (HeapPage)Database.getBufferPool().getPage(
                        tid,
                        new HeapPageId(getId(),currentPageIndex),
                        Permissions.READ_ONLY);
                // Get the iterator in the first HeapPage
                this.tupleIterator = this.currentPage.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(this.tupleIterator == null){
                    return false;
                }
                if(this.tupleIterator.hasNext()){
                    return true;
                }else {
                    /*
                      If the iterator in this page is at the end
                      then test if the file has a next page
                      If so change the currentPage to the next page and update tupleIterator
                      then return tupleIterator.hasNext()
                      otherwise return false
                     */
                    if(this.currentPageIndex != numPages() -1){
                        this.currentPageIndex++;
                        this.currentPage = (HeapPage)Database.getBufferPool().getPage(
                                tid,
                                new HeapPageId(getId(),currentPageIndex),
                                Permissions.READ_ONLY);
                        this.tupleIterator = this.currentPage.iterator();
                        return this.tupleIterator.hasNext();
                    }else{
                        return false;
                    }
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(this.tupleIterator == null){
                    throw new NoSuchElementException();
                }
                if(this.tupleIterator.hasNext()){
                    return this.tupleIterator.next();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                this.tupleIterator = null;
                this.currentPage = null;
            }
        };
    }


}

