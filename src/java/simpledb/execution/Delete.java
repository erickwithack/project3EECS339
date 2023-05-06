package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */

     private OpIterator child;
     private TupleDesc returnTD;
     private TransactionId tid;
     private boolean processed=false;

    public Delete(TransactionId t, OpIterator child) {
        // TODO: some code goes here
        this.child = child;
        this.tid = t;
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        this.returnTD = new TupleDesc(typeAr);
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (processed)
        return null;
    
    int count = 0;
    while (child.hasNext()) {
        Tuple t = child.next();
        try {
            Database.getBufferPool().deleteTuple(tid, t);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        count++;
    }

    // finished scanning
    // generate a new "delete count" tuple
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
