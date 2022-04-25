package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden, Zetong Zhao
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;
    private byte[] bytes;
    private BufferPool bufferPool;
    private int numPages = 0;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.bufferPool = Database.getBufferPool();
        updateBytes(f);
    }

    private void updateBytes(File f) {
        try {
            bytes = Utility.readFileBytes(f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        HeapPageId currentPid = null;
        currentPid = (HeapPageId) pid;
        assert currentPid != null;
        int beginPosition = pageSize * currentPid.getPageNumber();
        byte[] currentByte = new byte[pageSize];
        System.arraycopy(bytes, beginPosition, currentByte, 0, pageSize);
        HeapPage heapPage = null;
        try {
            heapPage = new HeapPage(currentPid, currentByte);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        HeapPageId id = (HeapPageId) page.getId();
        int size = BufferPool.getPageSize();
        int pageNumber = id.getPageNumber();
        byte[] data = page.getPageData();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rw");
            randomAccessFile.seek((long) pageNumber * size);
            randomAccessFile.write(data);
            randomAccessFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        updateBytes(f);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> changedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) bufferPool.getPage(tid, heapPageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                changedPages.add(page);
                return changedPages;
            }
        }
        HeapPageId heapPageId = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(heapPageId, new byte[BufferPool.getPageSize()]);
        newPage.insertTuple(t);
        changedPages.add(newPage);
        writePage(newPage);
        return changedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> changedPage = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);

        changedPage.add(page);
        return changedPage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(getId(), tid, Permissions.READ_ONLY, numPages());
    }
}

