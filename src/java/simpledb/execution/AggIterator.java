package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

import static simpledb.execution.Aggregator.NO_GROUPING;

public class AggIterator implements OpIterator {
    private Map<Field, List<Field>> tmpResult;
    private Aggregator.Op what;
    private List<Tuple> result;
    private TupleDesc td;
    private Iterator<Tuple> iterator;

    public AggIterator(Map<Field, List<Field>> tmpResult, Aggregator.Op what,
                       int gbfield, Type gbfieldtype, String nameOfGbField, String nameOfAggField) {
        this.tmpResult = tmpResult;
        this.what = what;
        result = new ArrayList<>();

        if (gbfield != NO_GROUPING) {
            Type[] typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
            String[] fieldAr = new String[]{nameOfGbField, nameOfAggField};
            td = new TupleDesc(typeAr, fieldAr);
        } else {
            Type[] typeAr = new Type[]{Type.INT_TYPE};
            String[] fieldAr = new String[]{nameOfAggField};
            td = new TupleDesc(typeAr, fieldAr);
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        switch (what) {
            case MIN -> {
                for (Field key : tmpResult.keySet()) {
                    List<Field> fields = tmpResult.get(key);
                    Field minField = fields.get(0);
                    for (int i = 0; i < fields.size(); i++) {
                        if (minField.compare(Predicate.Op.GREATER_THAN, fields.get(i))) {
                            minField = fields.get(i);
                        }
                    }

                    addFieldToResult(key, minField);
                }
            }
            case MAX -> {
                for (Field key : tmpResult.keySet()) {
                    List<Field> fields = tmpResult.get(key);
                    Field maxField = fields.get(0);
                    for (int i = 0; i < fields.size(); i++) {
                        if (maxField.compare(Predicate.Op.LESS_THAN, fields.get(i))) {
                            maxField = fields.get(i);
                        }
                    }

                    addFieldToResult(key, maxField);
                }
            }
            case SUM -> {
                for (Field key : tmpResult.keySet()) {
                    List<Field> fields = tmpResult.get(key);
                    int sum = 0;
                    for (Field f : fields) {
                        sum += ((IntField) f).getValue();
                    }
                    Field sumField = new IntField(sum);

                    addFieldToResult(key, sumField);
                }
            }
            case COUNT -> {
                for (Field key : tmpResult.keySet()) {
                    int size = tmpResult.get(key).size();
                    Field countField = new IntField(size);

                    addFieldToResult(key, countField);
                }
            }
            case AVG -> {
                for (Field key : tmpResult.keySet()) {
                    List<Field> fields = tmpResult.get(key);
                    int sum = 0;
                    for (Field f : fields) {
                        sum += ((IntField) f).getValue();
                    }
                    Field avgField = new IntField(sum / fields.size());

                    addFieldToResult(key, avgField);
                }
            }
        }

        iterator = result.iterator();
    }

    private void addFieldToResult(Field key, Field fieldToBeAdd) {
        Tuple tempTuple = new Tuple(td);
        if (td.numFields() == 1) {
            tempTuple.setField(0, fieldToBeAdd);
        } else {
            tempTuple.setField(0, key);
            tempTuple.setField(1, fieldToBeAdd);
        }
        result.add(tempTuple);
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return iterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        iterator = result.iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void close() {
        iterator = null;
    }
}
