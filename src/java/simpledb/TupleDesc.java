package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private LinkedList<TDItem> filedTDItems;

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return filedTDItems.iterator();
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
        // some code goes here
        filedTDItems = new LinkedList<>();
        for (int i = 0; i < typeAr.length; i++) {
            filedTDItems.add(new TDItem(typeAr[i], fieldAr[i]));
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
        // some code goes here
        filedTDItems = new LinkedList<>();
        for (Type type : typeAr) {
            filedTDItems.add(new TDItem(type, null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        if (filedTDItems != null) {
            return filedTDItems.size();
        }
        return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= 0 && i < filedTDItems.size()) {
            return filedTDItems.get(i).fieldName;
        } else {
            throw new NoSuchElementException();
        }
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
        // some code goes here
        if (i >= 0 && i < filedTDItems.size()) {
            return filedTDItems.get(i).fieldType;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < filedTDItems.size(); i++) {
            if (filedTDItems.get(i).fieldName!=null&&filedTDItems.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sum = 0;
        for (int i = 0; i < filedTDItems.size(); i++) {
            sum += getFieldType(i).getLen();
        }
        return sum;
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
        // some code goes here
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldAr = new String[td1.numFields() + td2.numFields()];
        Iterator<TDItem> iter1 = td1.iterator();
        Iterator<TDItem> iter2 = td2.iterator();
        int index = 0;
        while (iter1.hasNext()) {
            TDItem next = iter1.next();
            typeAr[index] = next.fieldType;
            fieldAr[index] = next.fieldName;
            index++;
        }
        while (iter2.hasNext()) {
            TDItem next = iter2.next();
            typeAr[index] = next.fieldType;
            fieldAr[index] = next.fieldName;
            index++;
        }
        return new TupleDesc(typeAr, fieldAr);
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

    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        TupleDesc tmp = (TupleDesc) o;
        if (tmp.numFields() == this.numFields()) {
            Iterator<TDItem> iter1 = this.iterator();
            Iterator<TDItem> iter2 = tmp.iterator();
            while (iter1.hasNext()) {
                if (iter1.next().fieldType != iter2.next().fieldType) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        // some code goes here
        StringBuilder builder = new StringBuilder();
        Iterator<TDItem> iter = this.iterator();
        if (iter.hasNext()) {
            TDItem first = iter.next();
            builder.append(first.fieldType).append("(").append(first.fieldName).append(")");
        }
        while (iter.hasNext()) {
            TDItem tmp = iter.next();
            builder.append(",").append(tmp.fieldType).append("(").append(tmp.fieldName).append(")");
        }
        return builder.toString();
    }
}
