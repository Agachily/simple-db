package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    // Define the list used for store the mapping relationship between the type and field name.
    private List<TDItem> tupleDesc = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public Type getFieldType() {
            return fieldType;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType && Objects.equals(fieldName, tdItem.fieldName);
        }
    }

    public List<TDItem> getTupleDesc() {
        return tupleDesc;
    }

    public void setTupleDesc(List<TDItem> tupleDesc) {
        this.tupleDesc = tupleDesc;
    }


    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tupleDesc.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length == fieldAr.length) {
            for (var i = 0; i < typeAr.length; i++) {
                TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
                tupleDesc.add(tdItem);
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        for (Type type : typeAr) {
            TDItem tdItem = new TDItem(type, null);
            this.tupleDesc.add(tdItem);
        }
    }

    public TupleDesc() {
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tupleDesc.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i > numFields()) {
            throw new NoSuchElementException();
        }
        return tupleDesc.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i > numFields()) {
            throw new NoSuchElementException();
        }
        return tupleDesc.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < tupleDesc.size(); i++) {
            String currentName = tupleDesc.get(i).fieldName;

            if (currentName != null) {
                if (tupleDesc.get(i).fieldName.equals(name)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        var length = 0;
        for (TDItem t : tupleDesc) {
            length += t.fieldType.getLen();
        }
        return length;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<TDItem> tupleDesc1 = td1.getTupleDesc();
        List<TDItem> tupleDesc2 = td2.getTupleDesc();
        ArrayList<TDItem> tdItems = new ArrayList<>(tupleDesc1);
        tdItems.addAll(tupleDesc2);
        TupleDesc tupleDesc = new TupleDesc();
        tupleDesc.tupleDesc = tdItems;
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o instanceof TupleDesc) {
            TupleDesc o1 = (TupleDesc) o;
            List<TDItem> tupleDescO1 = o1.getTupleDesc();
            if (this.numFields() == o1.numFields()) {
                for (var i = 0; i < this.tupleDesc.size(); i++) {
                    if (!this.tupleDesc.get(i).equals(tupleDescO1.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tupleDesc);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numFields(); i++) {
            builder.append(tupleDesc.get(i).fieldType + "(" + tupleDesc.get(i).fieldName + ")" + ", ");
        }
        return builder.toString();
    }
}
