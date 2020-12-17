package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            Comparator<Record> left_Compare = new LeftRecordComparator();
            Comparator<Record> right_Compare = new RightRecordComparator();
            SortOperator leftSort = new SortOperator(getTransaction(), getLeftTableName(), left_Compare);
            SortOperator rightSort = new SortOperator(getTransaction(), getRightTableName(), right_Compare);
            leftIterator = getTableIterator(leftSort.sort());
            rightIterator = getTableIterator(rightSort.sort());
            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
            rightIterator.markPrev();
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        private void fetchNextRecord() {
            // TODO(proj3_part1): implement
            if (leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
            nextRecord = null;
            do {
                if (rightRecord == null) {
                    rightIterator.reset();
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    leftIterator.next();
                }
                DataBox l_Join = leftRecord.getValues().get(getLeftColumnIndex());
                DataBox r_Join = rightRecord.getValues().get(getRightColumnIndex());
                while (l_Join.compareTo(r_Join) < 0) {
                    if (marked) {
                        rightIterator.reset();
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                        if (rightRecord == null) {rightIterator.reset();}
                        r_Join = rightRecord.getValues().get(getRightColumnIndex());
                    }
                    leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    if (leftRecord == null) {break;}
                    l_Join = leftRecord.getValues().get(getLeftColumnIndex());
                    marked = false;
                }
                while (l_Join.compareTo(r_Join) > 0) {
                    if (marked) {
                        rightIterator.reset();
                        marked=false;
                    }
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    if (rightRecord == null) {rightIterator.reset();}
                    r_Join = rightRecord.getValues().get(getRightColumnIndex());
                }
                if (l_Join.compareTo(r_Join) == 0) {
                    if (!marked) {
                        marked = true;
                        rightIterator.markPrev();
                    }
                    List<DataBox> l_Vals = new ArrayList<>(leftRecord.getValues());
                    List<DataBox> r_Vals = new ArrayList<>(rightRecord.getValues());
                    l_Vals.addAll(r_Vals);
                    nextRecord = new Record(l_Vals);
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    if (rightRecord == null) {rightIterator.reset();}
                }
            } while (!hasNext());

        }


        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
