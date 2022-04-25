package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The class is used for generate a iterator for the HeapFile object
 */
public class HeapFileIterator implements  DbFileIterator{
    private int tableId;
    private TransactionId tid;
    private Permissions perm;
    private int pageNumber;
    private int currentPageNumber;
    private HeapPage page;
    Iterator<Tuple> iterator;

    HeapFileIterator(int tableId, TransactionId tid, Permissions perm, int pageNumber) {
        this.tableId = tableId;
        this.tid = tid;
        this.perm = perm;
        this.pageNumber = pageNumber;
        this.currentPageNumber = 0;
    }

    private Page obtainCurrentPage(int tableId, TransactionId tid, Permissions perm, int pageNumber) {
        Page page = null;
        BufferPool bufferPool = Database.getBufferPool();
        HeapPageId heapPageId = new HeapPageId(tableId, currentPageNumber);
        try {
            page = bufferPool.getPage(tid, heapPageId, perm);
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }
        return page;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        currentPageNumber = 0;
        page = (HeapPage) obtainCurrentPage(tableId, tid, perm, currentPageNumber);
        iterator = page.iterator();
        currentPageNumber++;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (iterator == null) {
            return false;
        }
        if (iterator.hasNext()) {
            return true;
        } else {
            for (int i = currentPageNumber; i < pageNumber; currentPageNumber++) {
                page = (HeapPage) obtainCurrentPage(tableId, tid, perm, currentPageNumber);
                if (page.iterator().hasNext()) {
                    iterator = page.iterator();
                    currentPageNumber++;
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (iterator == null) {
            throw new NoSuchElementException();
        }
        return iterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    @Override
    public void close() {
        iterator = null;
    }
}
