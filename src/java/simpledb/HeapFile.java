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
            raf.seek((long) pid.getPageNumber() *BufferPool.getPageSize());
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
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (this.file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
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
                /**
                 * Get the first HeapPage in this DbFile
                 * Must get the page though the BufferPool
                 * in order to add the page into the BufferPool
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
                    /**
                     * If the iterator in this page is at the end
                     * then test if the file has a next page
                     * If so change the currentPage to the next page and update tupleIterator
                     * then return tupleIterator.hasNext()
                     * otherwise return false
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

