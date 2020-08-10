package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {
        assert(run != null);
        // must initialize runList or else null is carried all the way
        List<Record> runList = new ArrayList<>();
        Iterator<Record> runIter = run.iterator();
        while(runIter.hasNext()) {
            runList.add(runIter.next());
        }
        Collections.sort(runList, comparator);
        assert(runList != null);
        Run toReturn = createRun();
        toReturn.addRecords(runList);
        return toReturn;
    }

    /**
     * Given a list of sorted runs, returns a NEW run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {

        assert(runs != null);

        // Merged run should be a PQ over <Record, Run_i> pairs;
        // PQ over the given comparator at end of this class
        PriorityQueue<Pair<Record, Integer>> mergedRunPQ =
                new PriorityQueue<>(new RecordPairComparator());
        Run mergedRun = createRun();

        // Add all run elements to PQ with their respective run number (loop index in this case)
        // This step does all of our sorting for us via minPQ
        for(int i = 0; i < runs.size(); i++) {
            if (runs.get(i).iterator().hasNext()) {
                Iterator<Record> runIter = runs.get(i).iterator();
                while (runIter.hasNext()) {
                    Pair<Record, Integer> recordRun = new Pair<>(runIter.next(), i);
                    mergedRunPQ.add(recordRun);
                }
            }
        }

        // Add the records to the newly created run
        while (!mergedRunPQ.isEmpty()) {
            Record toAdd = mergedRunPQ.poll().getFirst();
            mergedRun.addRecord(toAdd.getValues());
        }

        return mergedRun;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time. It is okay for the last sorted run
     * to use less than (numBuffers - 1) input runs if N is not a
     * perfect multiple.
     */
    public List<Run> mergePass(List<Run> runs) {

        if (runs.isEmpty()){
            return runs;
        }

        int maxRuns = numBuffers - 1;
        List<Run> runList = new ArrayList<>();

        // Need to iterate over blocks of length = maxRuns
        for(int lower = 0; lower < runs.size(); lower += maxRuns) {
            int upper = Math.min(runs.size(), lower + maxRuns);
            List<Run> runSubList = runs.subList(lower, upper);
            Run toAdd = mergeSortedRuns(runSubList);
            runList.add(toAdd);
            if (upper < maxRuns) {
                break;
            }
        }
        return runList;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {
        // Sorting Lessons:
        // 1. Pass 0: grab all records from the table in chunks of size NUMBUFFERS; for each chunk of records, create a sorted run
        // 2. Pass 1 - N: repeatedly call mergePass until we have a single run (yay abstraction!)

        List<Run> runList = new ArrayList<>();
        BacktrackingIterator<Page> myPages = transaction.getPageIterator(tableName);
        if (!myPages.hasNext()) {
            return tableName;
        }

        while (myPages.hasNext()) {
            // Step 1: Pass 0
            BacktrackingIterator<Record> records = transaction.getBlockIterator(tableName, myPages, numBuffers);
            Run r = createRunFromIterator(records);
            // Forgot to sort :>
            r = sortRun(r);
            runList.add(r);
        }

        // Step 2: Pass 1 - N
        int runSize = runList.size();
        while(runSize > 1) {
            runList = mergePass(runList);
            runSize = runList.size();
        }

        Run toReturn = runList.get(0);
        return toReturn.tableName();
    }

    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

