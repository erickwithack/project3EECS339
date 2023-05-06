package simpledb.execution;

import simpledb.transaction.TransactionId;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.common.Type;
import simpledb.common.Utility;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    

    private OpIterator child;
    private int tableId;
    private TransactionId tid;
    private TupleDesc returnTD;
    private boolean processed=false;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // TODO: some code goes here
        this.child = child;
        this.tableId = tableId;
        this.tid = t;

        // verify that TupleDescriptors are the same
        if (!child.getTupleDesc().equals(
                Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("incompatible tuple descriptors for Insert");

        // we return a 1-field tuple
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        returnTD = new TupleDesc(typeAr);
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return returnTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        child.close();
        child.open();
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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (processed)
            return null;

        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, t);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            count++;
        }

        // finished scanning
        // generate a new "insert count" tuple
        Tuple tup = new Tuple(returnTD);
        tup.setField(0, new IntField(count));
        processed=true;
        return tup;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        this.child = children[0];
    }
}
