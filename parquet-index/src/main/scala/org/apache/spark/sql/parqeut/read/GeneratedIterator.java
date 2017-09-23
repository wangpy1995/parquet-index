package org.apache.spark.sql.parqeut.read;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.types.UTF8String;

public final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
    private Object[] references;
    private scala.collection.Iterator[] inputs;
    private scala.collection.Iterator scan_input;
    private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows;
    private org.apache.spark.sql.execution.metric.SQLMetric scan_scanTime;
    private long scan_scanTime1;
    private org.apache.spark.sql.execution.vectorized.ColumnarBatch scan_batch;
    private int scan_batchIdx;
    private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance0;
    private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance1;
    private UnsafeRow scan_result;
    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder scan_holder;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter scan_rowWriter;

    public GeneratedIterator(Object[] references) {
        this.references = references;
    }

    public void init(int index, scala.collection.Iterator[] inputs) {
        partitionIndex = index;
        this.inputs = inputs;
        scan_input = inputs[0];
        this.scan_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
        this.scan_scanTime = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
        scan_scanTime1 = 0;
        scan_batch = null;
        scan_batchIdx = 0;
        scan_colInstance0 = null;
        scan_colInstance1 = null;
        scan_result = new UnsafeRow(2);
        this.scan_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(scan_result, 32);
        this.scan_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(scan_holder, 2);

    }

    private void scan_nextBatch() throws java.io.IOException {
        long getBatchStart = System.nanoTime();
        if (scan_input.hasNext()) {
            scan_batch = (org.apache.spark.sql.execution.vectorized.ColumnarBatch) scan_input.next();
            scan_numOutputRows.add(scan_batch.numRows());
            scan_batchIdx = 0;
            scan_colInstance0 = scan_batch.column(0);
            scan_colInstance1 = scan_batch.column(1);

        }
        scan_scanTime1 += System.nanoTime() - getBatchStart;
    }

    protected void processNext() throws java.io.IOException {
        if (scan_batch == null) {
            scan_nextBatch();
        }
        while (scan_batch != null) {
            int scan_numRows = scan_batch.numRows();
            int scan_localEnd = scan_numRows - scan_batchIdx;
            for (int scan_localIdx = 0; scan_localIdx < scan_localEnd; scan_localIdx++) {
                int scan_rowIdx = scan_batchIdx + scan_localIdx;
                boolean scan_isNull = scan_colInstance0.isNullAt(scan_rowIdx);
                UTF8String scan_value = scan_isNull ? null : (scan_colInstance0.getUTF8String(scan_rowIdx));
                boolean scan_isNull1 = scan_colInstance1.isNullAt(scan_rowIdx);
                int scan_value1 = scan_isNull1 ? -1 : (scan_colInstance1.getInt(scan_rowIdx));
                scan_holder.reset();

                scan_rowWriter.zeroOutNullBytes();

                if (scan_isNull) {
                    scan_rowWriter.setNullAt(0);
                } else {
                    scan_rowWriter.write(0, scan_value);
                }

                if (scan_isNull1) {
                    scan_rowWriter.setNullAt(1);
                } else {
                    scan_rowWriter.write(1, scan_value1);
                }
                scan_result.setTotalSize(scan_holder.totalSize());
                append(scan_result);
                if (shouldStop()) {
                    scan_batchIdx = scan_rowIdx + 1;
                    return;
                }
            }
            scan_batchIdx = scan_numRows;
            scan_batch = null;
            scan_nextBatch();
        }
        scan_scanTime.add(scan_scanTime1 / (1000 * 1000));
        scan_scanTime1 = 0;
    }
}
