package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aggregateField;
    private int groupField;
    private Aggregator.Op operator;

    private TupleDesc td;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // some code goes here
        this.child = child;
        this.aggregateField = afield;
        this.groupField = gfield;
        this.operator = aop;
        this.td = child.getTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        return this.groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        return td.getFieldName(groupField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return this.aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // some code goes here
        return td.getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    private OpIterator iterator;

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        // some code goes here
        child.open();
        generate();
    }

    private void generate() throws TransactionAbortedException, DbException {
        super.open();
        Type groupFieldType = null;
        if (groupField != Aggregator.NO_GROUPING) {
            groupFieldType = this.td.getFieldType(groupField);
        }
        if(td.getFieldType(aggregateField).equals(Type.INT_TYPE)) {
            IntegerAggregator tmp = new IntegerAggregator(groupField, groupFieldType, aggregateField, operator);
            while(child.hasNext()) {
                Tuple t = child.next();
                tmp.mergeTupleIntoGroup(t);
            }
            iterator =  tmp.iterator();
        }
        else {
            StringAggregator tmp = new StringAggregator(groupField, groupFieldType, aggregateField, operator);
            while(child.hasNext()) {
                Tuple t = child.next();
                tmp.mergeTupleIntoGroup(t);
            }
            iterator = tmp.iterator();
        }
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.iterator.hasNext()?this.iterator.next():null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        this.iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void close() {
	    // some code goes here
        this.child.close();
        this.iterator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
    
}
