package simpledb.execution;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    private Op what;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private HashMap<String, AggregateFields> groups;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.what = what;
        this.gbfieldtype = gbfieldtype;
        this.gbfield = gbfield;
        this.afield = afield;
        this.groups = new HashMap<String, AggregateFields>();
    }

  
    
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        String groupStr = "";
        if (gbfield != NO_GROUPING) {
            groupStr = tup.getField(gbfield).toString();
        }
        AggregateFields agg = groups.get(groupStr);
        if (agg == null)
            agg = new AggregateFields(groupStr);

        int x = ((IntField) tup.getField(afield)).getValue();

        agg.count++;
        agg.sum += x;
        if (x < agg.min){
            agg.min = x;
        } 

        if (x > agg.max){
            agg.max = x;
        }
        
        if (what==Op.SC_AVG)
            agg.sumCount+=((IntField) tup.getField(afield+1)).getValue();

        groups.put(groupStr, agg);
    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        LinkedList<Tuple> result = new LinkedList<Tuple>();
        int aggField = 1;
        TupleDesc td;

        if (gbfield == NO_GROUPING) {
            if (what==Op.SUM_COUNT)
        	td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
            else
        	td = new TupleDesc(new Type[] { Type.INT_TYPE });
            aggField = 0;
        } else {
            if (what==Op.SUM_COUNT)
        	td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE, Type.INT_TYPE});
            else
        	td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
        }

        // iterate over groups and create summary tuples
        for (String groupVal : groups.keySet()) {
            AggregateFields agg = groups.get(groupVal);
            Tuple tup = new Tuple(td);

            if (gbfield != NO_GROUPING) {
                if (gbfieldtype == Type.INT_TYPE)
                    tup.setField(0, new IntField(new Integer(groupVal)));
                else
                    tup.setField(0, new StringField(groupVal, Type.STRING_LEN));
            }
            switch (what) {
            case MIN:
                tup.setField(aggField, new IntField(agg.min));
                break;
            case MAX:
                tup.setField(aggField, new IntField(agg.max));
                break;
            case SUM:
                tup.setField(aggField, new IntField(agg.sum));
                break;
            case COUNT:
                tup.setField(aggField, new IntField(agg.count));
                break;
            case AVG:
                tup.setField(aggField, new IntField(agg.sum / agg.count));
                break;
            case SUM_COUNT:
        	tup.setField(aggField, new IntField(agg.sum));
        	tup.setField(aggField+1, new IntField(agg.count));
        	break;
            case SC_AVG:
        	tup.setField(aggField, new IntField(agg.sum / agg.sumCount));
        	break;
            }

            result.add(tup);
        }

        OpIterator retVal = null;
        retVal = new TupleIterator(td, Collections.unmodifiableList(result));
        return retVal;
    
    }

    private class AggregateFields {
        public String groupVal;
        public int min, max, sum, count, sumCount;

        public AggregateFields(String groupVal) {
            this.groupVal = groupVal;
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
            sum = count = sumCount = 0;
        }
    }

}
