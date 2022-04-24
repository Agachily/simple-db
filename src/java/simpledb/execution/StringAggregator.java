package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private String nameOfGbField;
    private String nameOfAggField;
    Map<Field, List<Field>> tmpResult;


    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        tmpResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField = null;
        Field aggField = tup.getField(afield);

        if (nameOfAggField == null) {
            nameOfAggField = tup.getTupleDesc().getFieldName(afield);
        }

        if (gbfield != NO_GROUPING) {
            groupByField = tup.getField(gbfield);

            if (nameOfGbField == null) {
                nameOfGbField = tup.getTupleDesc().getFieldName(gbfield);
            }
        }

        if (tmpResult.containsKey(groupByField)) {
            tmpResult.get(groupByField).add(aggField);
        } else {
            List<Field> fields = new ArrayList<>();
            fields.add(aggField);
            tmpResult.put(groupByField, fields);
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
        return new AggIterator(tmpResult, what, gbfield, gbfieldtype, nameOfGbField, nameOfAggField);
    }

}
