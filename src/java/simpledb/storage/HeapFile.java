package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private TupleDesc td;
	private File file;
	private int tableid;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
       this.file = f;
		this.td = td;
		tableid=file.getAbsoluteFile().hashCode();
    }


    private volatile int lastEmptyPage = -1;

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
        return file.getAbsoluteFile().hashCode();
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
        //TODO some code goes here
        HeapPage resHP;
		try {
			// HeapPage resHP;
			RandomAccessFile rfile = new RandomAccessFile(file, "r");
			int psize = BufferPool.getPageSize();
			int indice = pid.getPageNumber() * psize;
			byte[] readdata = new byte[psize];

			// get data
			rfile.seek(indice);
			rfile.read(readdata, 0, psize);
			rfile.close();
			resHP = new HeapPage((HeapPageId) pid, readdata);

		} catch (IOException e) {
			e.printStackTrace();
			throw new NoSuchElementException();
		}
		return resHP;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        HeapPage p = (HeapPage) page;
		byte[] data = p.getPageData();
		RandomAccessFile rf = new RandomAccessFile(file, "rw");
		rf.seek(p.getId().getPageNumber() * BufferPool.getPageSize());
		rf.write(data);
		rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // XXX: this seems to be rounding it down. isn't that wrong?
        // XXX: (marcua) no - we only ever write full pages
        return (int) (file.length() / BufferPool.getPageSize());
    }
   

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        ArrayList<Page> dirtypages = new ArrayList<Page>();

		// find the first page with a free slot in it
		int i = 0;
		if (lastEmptyPage != -1)
			i = lastEmptyPage;
		for (; i < numPages(); i++) {
			Debug.log(4, "HeapFile.addTuple: checking free slots on page %d of table %d", i, tableid);
			HeapPageId pid = new HeapPageId(tableid, i);
			HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

			if (p.getNumUnusedSlots() == 0) {
				Debug.log(4, "HeapFile.addTuple: no free slots on page %d of table %d", i, tableid);

				if (lastEmptyPage != -1) {
					lastEmptyPage = -1;
					break;
				}
				continue;
			}
			Debug.log(4, "HeapFile.addTuple: %d free slots in table %d", p.getNumUnusedSlots(), tableid);
			p.insertTuple(t);
			lastEmptyPage = p.getId().getPageNumber();
			// System.out.println("nfetches = " + nfetches);
			dirtypages.add(p);
			return dirtypages;
		}
		synchronized (this) {
			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true));
			byte[] emptyData = HeapPage.createEmptyPageData();
			bw.write(emptyData);
			bw.close();
		}

		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableid, numPages() - 1),
				Permissions.READ_WRITE);
		p.insertTuple(t);
		lastEmptyPage = p.getId().getPageNumber();
		// System.out.println("nfetches = " + nfetches);
		dirtypages.add(p);
		return dirtypages;
    
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
        new HeapPageId(tableid, t.getRecordId().getPageId().getPageNumber()), Permissions.READ_WRITE);
        p.deleteTuple(t);
        ArrayList<Page> pages = new ArrayList<Page>();
        pages.add(p);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile
 */
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    int curpgno = 0;

    final TransactionId tid;
    final HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open() {
        curpgno = -1;
    }

    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curpgno < hf.numPages() - 1) {
            curpgno++;
            HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
            HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
                    curpid, Permissions.READ_ONLY);
            it = curp.iterator();
            if (!it.hasNext())
                it = null;
        }

        if (it == null)
            return null;
        return it.next();
    }

    public void rewind() {
        close();
        open();
    }

    public void close() {
        super.close();
        it = null;
        curpgno = Integer.MAX_VALUE;
    }
}
