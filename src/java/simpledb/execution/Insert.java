package simpledb.execution;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private static Catalog catalog;
    private static BufferPool bufferPool;
    private TupleDesc td;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        catalog = Database.getCatalog();
        bufferPool = Database.getBufferPool();
        TupleDesc targetTd = catalog.getTupleDesc(tableId);
        TupleDesc sourceTd = child.getTupleDesc();
        if (!sourceTd.equals(targetTd)) {
            throw new DbException("The TupleDesc of child differs from table into which we are to insert");
        }
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        // Set the TupleDesc of the return tuple, since it counts the changed tuple, so it has to be int
        td = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException{
        int count = 0;
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                bufferPool.insertTuple(t, tableId, next);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }
        Tuple tuple = new Tuple(td);
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
