package org.example;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

class SpecificSafeProjection extends org.apache.spark.sql.catalyst.expressions.codegen.BaseProjection {

    private Object[] references;
    private InternalRow mutableRow;

    public static void main(String[] args) {
        SpecificSafeProjection s = new SpecificSafeProjection(null);
    }


    public SpecificSafeProjection(Object[] references) {
        this.references = references;
        mutableRow = (InternalRow) references[references.length - 1];

    }

    public void initialize(int partitionIndex) {

    }

    @Override
    public InternalRow apply(InternalRow i) {
        Object[] values_0 = new Object[2];

        boolean isNull_1 = i.isNullAt(0);
        long value_1 = isNull_1 ? -1L : (i.getLong(0));
        if (isNull_1) {
            values_0[0] = null;
        } else {
            values_0[0] = value_1;
        }

        boolean isNull_3 = i.isNullAt(1);
        UTF8String value_3 = isNull_3 ? null : (i.getUTF8String(1));
        boolean isNull_2 = true;
        java.lang.String value_2 = null;
        if (!isNull_3) {

            isNull_2 = false;
            if (!isNull_2) {

                Object funcResult_0 = null;
                funcResult_0 = value_3.toString();
                value_2 = (java.lang.String) funcResult_0;

            }
        }
        if (isNull_2) {
            values_0[1] = null;
        } else {
            values_0[1] = value_2;
        }

        final org.apache.spark.sql.Row value_0 = new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(values_0, ((org.apache.spark.sql.types.StructType) references[0] /* schema */));
        if (false) {
            mutableRow.setNullAt(0);
        } else {

            mutableRow.update(0, value_0);
        }

        return mutableRow;
    }
}
