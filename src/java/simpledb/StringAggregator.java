package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op operator;

    private HashMap<Field, Integer> groupCount;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT){
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.operator = what;
        this.groupCount = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        aggregatorCount(tup);
    }

    private void aggregatorCount(Tuple tup) {
        // case NO_GROUPING
        if(groupByField == Aggregator.NO_GROUPING){
            int count = 0;
            if(groupCount.containsKey(null)){
                count = groupCount.get(null);
            }
            count++;
            groupCount.put(null, count);
        }
        // case GROUPING
        else{
            Field groupField = tup.getField(groupByField);
            int count = 0;
            if(groupCount.containsKey(groupField)){
                count = groupCount.get(groupField);
            }
            count++;
            groupCount.put(groupField, count);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // case NO_GROUPING
        if(groupByField == Aggregator.NO_GROUPING){
            return new OpIterator() {

                Iterator<Integer> aggregateIterator;
                TupleDesc tupleDesc;

                @Override
                public void open() throws DbException, TransactionAbortedException {
                    aggregateIterator = groupCount.values().iterator();
                    tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
                }

                @Override
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    if(aggregateIterator == null) {
                        return false;
                    }
                    return aggregateIterator.hasNext();
                }

                @Override
                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    if(aggregateIterator == null) {
                        throw new NoSuchElementException();
                    }
                    if(!aggregateIterator.hasNext()){
                        Tuple tuple = new Tuple(tupleDesc);
                        tuple.setField(0, new IntField(aggregateIterator.next()));
                        return tuple;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                public void rewind() throws DbException, TransactionAbortedException {
                    close();
                    open();
                }

                @Override
                public TupleDesc getTupleDesc() {
                    return tupleDesc;
                }

                @Override
                public void close() {
                    aggregateIterator = null;
                    tupleDesc = null;
                }
            };
        }
        // case GROUPING
        else{
            return new OpIterator() {

                Iterator<Field> groupIterator;
                Iterator<Integer> aggregateIterator;
                TupleDesc tupleDesc;

                @Override
                public void open() throws DbException, TransactionAbortedException {
                    groupIterator = groupCount.keySet().iterator();
                    aggregateIterator = groupCount.values().iterator();
                    tupleDesc = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE});
                }

                @Override
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    if(groupIterator == null) {
                        return false;
                    }
                    return groupIterator.hasNext();
                }

                @Override
                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    if(groupIterator == null) {
                        throw new NoSuchElementException();
                    }
                    if(groupIterator.hasNext()){
                        Tuple tuple = new Tuple(tupleDesc);
                        tuple.setField(0, groupIterator.next());
                        tuple.setField(1, new IntField(aggregateIterator.next()));
                        return tuple;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                public void rewind() throws DbException, TransactionAbortedException {
                    close();
                    open();
                }

                @Override
                public TupleDesc getTupleDesc() {
                    return tupleDesc;
                }

                @Override
                public void close() {
                    groupIterator = null;
                    aggregateIterator = null;
                    tupleDesc = null;
                }
            };
        }
    }
}
