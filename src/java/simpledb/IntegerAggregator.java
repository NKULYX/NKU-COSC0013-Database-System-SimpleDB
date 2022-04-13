package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op operator;

    private HashMap<Field, Integer> groupResult;
    private HashMap<Field, Integer> groupCount;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.operator = what;
        this.groupResult = new HashMap<Field, Integer>();
        this.groupCount = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        switch (this.operator) {
            case MIN:
                aggregatorMin(tup);
                break;
            case MAX:
                aggregatorMax(tup);
                break;
            case SUM:
                aggregatorSum(tup);
                break;
            case AVG:
                aggregatorAvg(tup);
                break;
            case COUNT:
                aggregatorCount(tup);
                break;
            default:
                throw new UnsupportedOperationException("Not Implemented");
        }
    }

    private void aggregatorMin(Tuple tup) {
        Field field = tup.getField(this.groupByField);
        int curData = ((IntField) tup.getField(this.aggregateField)).getValue();
        // case NO_GROUPING
        if(groupByField == NO_GROUPING) {
            // groupResult is still empty
            if(groupResult.size() == 0) {
                groupResult.put(null, curData);
                groupCount.put(null,1);
            } else {
                int min = groupResult.get(null);
                int cnt = groupCount.get(null);
                if(min > curData) {
                    groupResult.put(null, curData);
                    groupCount.put(null,cnt+1);
                }
            }
        }
        // case GROUPING
        else{
            // groupResult contains the current field
            if(groupResult.containsKey(field)) {
                int min = groupResult.get(field);
                int cnt = groupCount.get(field);
                if(min > curData) {
                    groupResult.put(field, curData);
                    groupCount.put(field,cnt+1);
                }
            }
            // groupResult does not contain the current field
            else {
                groupResult.put(field, curData);
                groupCount.put(field,1);
            }
        }
    }

    private void aggregatorMax(Tuple tup) {
        Field field = tup.getField(this.groupByField);
        int curData = ((IntField) tup.getField(this.aggregateField)).getValue();
        // case NO_GROUPING
        if(groupByField == NO_GROUPING) {
            // groupResult is still empty
            if(groupResult.size() == 0) {
                groupResult.put(null, curData);
                groupCount.put(null,1);
            } else {
                int max = groupResult.get(null);
                int cnt = groupCount.get(null);
                if(max < curData) {
                    groupResult.put(null, curData);
                    groupCount.put(null,cnt+1);
                }
            }
        }
        // case GROUPING
        else{
            // groupResult contains the current field
            if(groupResult.containsKey(field)) {
                int max = groupResult.get(field);
                int cnt = groupCount.get(field);
                if(max < curData) {
                    groupResult.put(field, curData);
                    groupCount.put(field,cnt+1);
                }
            }
            // groupResult does not contain the current field
            else {
                groupResult.put(field, curData);
                groupCount.put(field,1);
            }
        }
    }

    private void aggregatorSum(Tuple tup) {
        Field field = tup.getField(this.groupByField);
        int curData = ((IntField) tup.getField(this.aggregateField)).getValue();
        // case NO_GROUPING
        if(groupByField == NO_GROUPING) {
            // groupResult is still empty
            if(groupResult.size() == 0) {
                groupResult.put(null, curData);
                groupCount.put(null,1);
            } else {
                int sum = groupResult.get(null);
                int cnt = groupCount.get(null);
                groupResult.put(null, sum + curData);
                groupCount.put(null,cnt+1);
            }
        }
        // case GROUPING
        else{
            // groupResult contains the current field
            if(groupResult.containsKey(field)) {
                int sum = groupResult.get(field);
                int cnt = groupCount.get(field);
                groupResult.put(field, sum + curData);
                groupCount.put(field,cnt+1);
            }
            // groupResult does not contain the current field
            else {
                groupResult.put(field, curData);
                groupCount.put(field,1);
            }
        }
    }

    private void aggregatorAvg(Tuple tup) {
        Field field = tup.getField(this.groupByField);
        int curData = ((IntField) tup.getField(this.aggregateField)).getValue();
        // case NO_GROUPING
        if(groupByField == NO_GROUPING) {
            // groupResult is still empty
            if(groupResult.size() == 0) {
                groupResult.put(null, curData);
                groupCount.put(null, 1);
            } else {
                int sum = groupResult.get(null);
                int count = groupCount.get(null);
                sum = sum  + curData;
                groupResult.put(null, sum);
                groupCount.put(null, count + 1);
            }
        }
        // case GROUPING
        else{
            // groupResult contains the current field
            if(groupResult.containsKey(field)) {
                int sum = groupResult.get(field);
                int count = groupCount.get(field);
                sum = sum  + curData;
                groupResult.put(field, sum);
                groupCount.put(field, count + 1);
            }
            // groupResult does not contain the current field
            else {
                groupResult.put(field, curData);
                groupCount.put(field, 1);
            }
        }
    }

    private void aggregatorCount(Tuple tup) {
        Field field = tup.getField(this.groupByField);
        // case NO_GROUPING
        if(groupByField == NO_GROUPING) {
            // groupResult is still empty
            if(groupResult.size() == 0) {
                groupResult.put(null, 1);
                groupCount.put(null, 1);
            } else {
                int count = groupResult.get(null);
                groupResult.put(null, count + 1);
                groupCount.put(null, count + 1);
            }
        }
        // case GROUPING
        else{
            // groupResult contains the current field
            if(groupResult.containsKey(field)) {
                int count = groupResult.get(field);
                groupResult.put(field, count + 1);
                groupCount.put(field, count + 1);
            }
            // groupResult does not contain the current field
            else {
                groupResult.put(field, 1);
                groupCount.put(field, 1);
            }
        }
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
        // some code goes here
        // case NO_GROUPING
        if(groupByField == NO_GROUPING) {
            return new OpIterator() {

                Iterator<Integer> aggregateIterator;
                Iterator<Integer> countIterator;
                TupleDesc tupleDesc;

                @Override
                public void open() throws DbException, TransactionAbortedException {
                    aggregateIterator = groupResult.values().iterator();
                    countIterator = groupCount.values().iterator();
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
                    if(aggregateIterator.hasNext()) {
                        Tuple tuple = new Tuple(tupleDesc);
                        if(operator==Op.AVG) {
                            tuple.setField(0, new IntField(aggregateIterator.next() / countIterator.next()));
                        } else{
                            tuple.setField(0, new IntField(aggregateIterator.next()));
                        }
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
        else {
            return new OpIterator() {

                Iterator<Field> groupIterator;
                Iterator<Integer> aggregateIterator;
                Iterator<Integer> countIterator;
                TupleDesc tupleDesc;

                @Override
                public void open() throws DbException, TransactionAbortedException {
                    groupIterator = groupResult.keySet().iterator();
                    aggregateIterator = groupResult.values().iterator();
                    countIterator = groupCount.values().iterator();
                    tupleDesc = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE});
                }

                @Override
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    if(groupIterator == null || aggregateIterator == null) {
                        return false;
                    }
                    return groupIterator.hasNext();
                }

                @Override
                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    if(groupIterator == null || aggregateIterator == null) {
                        throw new NoSuchElementException();
                    }
                    if(groupIterator.hasNext()) {
                        Tuple tuple = new Tuple(tupleDesc);
                        tuple.setField(0, groupIterator.next());
                        if(operator==Op.AVG) {
                            tuple.setField(1, new IntField(aggregateIterator.next() / countIterator.next()));
                        } else{
                            tuple.setField(1, new IntField(aggregateIterator.next()));
                        }
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
