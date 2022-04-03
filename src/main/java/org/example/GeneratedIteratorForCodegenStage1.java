package org.example;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
    private Object[] references;
    private scala.collection.Iterator[] inputs;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] scan_mutableStateArray_1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];
    private scala.collection.Iterator[] scan_mutableStateArray_0 = new scala.collection.Iterator[1];

    public GeneratedIteratorForCodegenStage1(Object[] references) {
        this.references = references;
    }

    public void init(int index, scala.collection.Iterator[] inputs) {
        partitionIndex = index;
        this.inputs = inputs;
        scan_mutableStateArray_0[0] = inputs[0];
        scan_mutableStateArray_1[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 32);

    }

    protected void processNext() throws java.io.IOException {
        while (scan_mutableStateArray_0[0].hasNext()) {
            InternalRow scan_row_0 = (InternalRow) scan_mutableStateArray_0[0].next();
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
            boolean scan_isNull_0 = scan_row_0.isNullAt(0);
            long scan_value_0 = scan_isNull_0 ? -1L : (scan_row_0.getLong(0));
            boolean scan_isNull_1 = scan_row_0.isNullAt(1);
            UTF8String scan_value_1 = scan_isNull_1 ? null : (scan_row_0.getUTF8String(1));
            scan_mutableStateArray_1[0].reset();

            scan_mutableStateArray_1[0].zeroOutNullBytes();

            if (scan_isNull_0) {
                scan_mutableStateArray_1[0].setNullAt(0);
            } else {
                scan_mutableStateArray_1[0].write(0, scan_value_0);
            }

            if (scan_isNull_1) {
                scan_mutableStateArray_1[0].setNullAt(1);
            } else {
                scan_mutableStateArray_1[0].write(1, scan_value_1);
            }
            append((scan_mutableStateArray_1[0].getRow()));
            if (shouldStop()) return;
        }
    }

}
