package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
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

            // Begin by SORTING left and right relations!!!
            SortOperator sortOpLeft = new SortOperator(SortMergeOperator.this.getTransaction(),
                    getLeftTableName(), new LeftRecordComparator());
            SortOperator sortOpRight = new SortOperator(SortMergeOperator.this.getTransaction(),
                    getRightTableName(), new RightRecordComparator());

            // Do not use the original relations to create iterators, use the sorted ones!
            String leftSorted = sortOpLeft.sort();
            String rightSorted = sortOpRight.sort();

            leftIterator = SortMergeOperator.this.getRecordIterator(leftSorted);
            rightIterator = SortMergeOperator.this.getRecordIterator(rightSorted);
            assert(leftIterator.hasNext());
            assert(rightIterator.hasNext());

            rightRecord = rightIterator.next();
            leftRecord = leftIterator.next();
            marked = false;

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }


        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        private void advanceRight() {
            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
        }

        private void advanceLeft() {
            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
        }

        private int leftRightCompare() {
            return new LeftRightRecordComparator().compare(leftRecord, rightRecord);
        }

        private void resetRightIter() {
            rightIterator.reset();
            assert(rightIterator.hasNext());
            rightRecord = rightIterator.next();
        }

        private void fetchNextRecord() {
            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch."); }
            nextRecord = null;
            while (!hasNext()) {
                if (leftRecord == null) throw new NoSuchElementException("All done");
                if (!marked) {
                    // left record LESS THAN right record
                    while (leftRightCompare() < 0 && leftIterator.hasNext()) {
                        advanceLeft();
                    }
                    // left record GREATER THAN right record
                    while (leftRightCompare() > 0 && rightIterator.hasNext()) {
                        advanceRight();
                    }
                    marked = true;
                    rightIterator.markPrev();
                }

                if (rightRecord != null && leftRightCompare() == 0) {
                    nextRecord = joinRecords(leftRecord, rightRecord);
                    advanceRight();
                } else {
                    resetRightIter();
                    advanceLeft();
                    marked = false;
                }
            }
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

        private class LeftRightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
