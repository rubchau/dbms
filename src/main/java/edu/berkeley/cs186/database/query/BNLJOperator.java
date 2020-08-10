package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The next record to return
        private Record nextRecord = null;
        // Added by me: current record on right page
        private Record rightRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            //this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            // "If there are no more pages in the left relation with records, both leftRecordIterator
            // and leftRecord should be set to null."
            if (!leftIterator.hasNext()) {
                leftRecordIterator = null;
                leftRecord = null;
                throw new NoSuchElementException("All Done!");
            }
            int blockSize = numBuffers - 2;
            assert(blockSize > 0);
            this.leftRecordIterator = BNLJOperator.this.getBlockIterator(getLeftTableName(), leftIterator, blockSize);
            assert(leftRecordIterator.hasNext());
            leftRecordIterator.markNext();
            leftRecord = this.leftRecordIterator.next();
            leftRecord = this.leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
            if (leftRecord != null) {
                leftRecordIterator.markPrev();
            }
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the RIGHT relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            //rightRecordIterator.markPrev();
            if (!rightIterator.hasNext()) {
                rightRecordIterator = null;
                rightRecord = null;
            } else {
                this.rightRecordIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), rightIterator, 1);
                rightRecord = rightRecordIterator.next();
                rightRecordIterator.markPrev();
                rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                if (rightRecordIterator != null) {
                    rightRecordIterator.markPrev();
                }
            }
        }

        private void resetRightRecordIterator() {
            this.rightRecordIterator.reset();
            assert(rightRecordIterator.hasNext());
            rightRecord = rightRecordIterator.next();
        }

        private void resetLeftRecordIterator() {
            this.leftRecordIterator.reset();
            assert(leftRecordIterator.hasNext());
            leftRecord = leftRecordIterator.next();
        }

        private void resetRightIter() {
            rightIterator.reset();
            assert(rightIterator.hasNext());
            fetchNextRightPage();
        }

        // indicesMatch() returns a bool on whether or not the current
        // right and left records should be joined
        private boolean indicesMatch() {
            DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
            DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
            return leftJoinValue.equals(rightJoinValue);
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            // TODO(proj3_part1): implement
            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch."); }
            this.nextRecord = null;

            // Implementation 2:
            /*while(!hasNext()) {
                // NEED for each block b-r in R
                if (leftRecordIterator != null) {
                    leftRecordIterator.markPrev();
                    if (rightRecordIterator != null) {
                        rightRecordIterator.markPrev();
                        Record rightRecord = rightRecordIterator.next();
                        if (rightRecord != null) {
                            DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                            DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                            if (leftJoinValue.equals(rightJoinValue)) {
                                this.nextRecord = joinRecords(leftRecord, rightRecord);
                            }
                            rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                        } else {
                            leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
                            resetRightRecordIterator();
                        }
                    } else {
                        fetchNextRightPage();
                        resetLeftRecordIterator();
                    }
                } else {
                    fetchNextLeftBlock();
                    resetRightRecordIterator();
                }
            }*/

            // Implementation 3:
            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch."); }
            this.nextRecord = null;
            // Implementation 2
            while (!hasNext()) {
                // Case 1: We are currently on the left and right record iterators that correspond to a record match
                // Case 2: We've iterated through an entire right block, no match --> increment the outer record (leftRecordIterator) and reset rightRecordIterator
                // Case 3: We've iterated through an entire left block, no match --> increment rightRecordIterator to a new page and reset the leftRecordIterator
                // Case 4: We've iterated through an entire left block and all P_s pages --> increment leftRecordIterator & reset rightIterator to its beginning!
                // Case 1: pretty much exactly from SNLJ:
                if (this.rightRecord != null) {
                    DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    // Join if the column indices match
                    if (leftJoinValue.equals(rightJoinValue)) {

                // Case 1: pretty much exactly from SNLJ:
                if (this.rightRecord != null) {
                    // Join if the column indices match
                    if (indicesMatch()) {
                        this.nextRecord = joinRecords(leftRecord, rightRecord);
                        //return;
                    }
                    // Increment inner relation
                    this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                }
                // Case 2: increment outer record and reset inner record
                //if (leftRecordIterator != null && leftRecordIterator.hasNext()) {
                else if (leftRecordIterator.hasNext()) {
                    leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
                    resetRightRecordIterator();
                }
                // Case 3: increment page of right relation, reset left record iterator
                //else if (rightIterator != null && rightIterator.hasNext()) {
                else if (rightIterator.hasNext()) {
                    fetchNextRightPage();
                    resetLeftRecordIterator();
                } // Case 4: outer block must be incremented, inner iterator completely reset
                else if (leftIterator.hasNext()){
                    fetchNextLeftBlock();
                    rightIterator = BNLJOperator.this.getPageIterator(getRightTableName());
                    fetchNextRightPage();
                }
            }

            // Implementation 1:
           /* while (!hasNext()) {
                while (leftRecordIterator != null) {
                    // "rewind" rightRecordIterator or rewind rightIterator HERE
                    rightRecordIterator.reset();
                    while (rightRecordIterator != null) {
                        Record rightRecord = rightRecordIterator.next();
                        if (rightRecord != null) {
                            DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                            DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                            if (leftJoinValue.equals(rightJoinValue)) {
                                this.nextRecord = joinRecords(leftRecord, rightRecord);
                            }
                            rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                        }
                        fetchNextRightPage();
                    }
                }
            }*/
                    // Extra IO incurred when fetching next right page in resetRightIter()
                    resetRightIter();
                }
                // If anyone is reading this: these next three lines actually saved my life.
                else {
                    throw new NoSuchElementException("All done.");
                }
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
    }
}
