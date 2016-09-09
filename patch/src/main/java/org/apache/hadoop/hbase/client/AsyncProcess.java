package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jh4j on 2016/9/8.
 */
public class AsyncProcess {

    protected static final Log LOG = LogFactory.getLog(AsyncProcess.class);
    protected final ClusterConnection connection;
    protected static final AtomicLong COUNTER = new AtomicLong();

    public AsyncProcess(ClusterConnection hc, Configuration conf, ExecutorService pool, boolean useGlobalErrors) {
        this.connection = hc;
        this.pool = pool;
        this.maxTotalConcurrentTasks = conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS);
        this.id = COUNTER.incrementAndGet();

        this.globalErrors = useGlobalErrors ? new BatchErrors() : null;
    }

    /**
     * The number of tasks simultaneously executed on the cluster.
     */
    protected final int maxTotalConcurrentTasks;
    public <CResult> AsyncRequestFuture submitAll(ExecutorService pool, TableName tableName, List<? extends Row> rows, Object callback, Object[] results) throws InterruptedException {

        List<Action<Row>> actions = new ArrayList<>(rows.size());

        int posInList = -1;
        NonceGenerator ng = this.connection.getNonceGenerator();

        for (Row r : rows){
            posInList++;
//            if (r instanceof Put)
            Action<Row>  action = new Action<Row>(r, posInList);
            setNonce(ng, r, action);
            actions.add(action);
        }

        AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(
                tableName, actions, ng.getNonceGroup(), getPool(pool), callback, results, results != null);

        ars.groupAndSendMultiAction(actions, 1);
        return ars;
    }

    private <CResult> AsyncRequestFutureImpl<CResult> createAsyncRequestFuture(
            TableName tableName, List<Action<Row>> actions, long nonceGroup, ExecutorService pool, Object callback,
            Object[] results, boolean needResults) {
        return new AsyncRequestFutureImpl<CResult>(
                tableName, actions, nonceGroup, getPool(pool), needResults, results, callback);
    }

    protected final ExecutorService pool;
    private ExecutorService getPool(ExecutorService pool) {
        if (pool != null)  return pool;
        if (this.pool != null) return this.pool;
        throw new RuntimeException("Neither AsyncProcess nor request have ExecutorService.");
    }

    private void setNonce(NonceGenerator ng, Row r, Action<Row> action) {
        if (!(r instanceof Append) && !(r instanceof Increment)) return;
        action.setNonce(ng.newNonce());
    }

    protected final long id;
    protected final AtomicLong tasksInProgress = new AtomicLong(0);
    /**
     * wait until the async does not have more than max tasks in progress.
     * @param max
     */
    private void waitForMaximumCurrentTasks(int max) throws InterruptedException {
        long lastLog = EnvironmentEdgeManager.currentTime();
        long currentInProgress, oldInProgress = Long.MAX_VALUE;
        while ((currentInProgress = this.tasksInProgress.get()) > max){
            if (oldInProgress != currentInProgress){
                long now = EnvironmentEdgeManager.currentTime();
                if (now > lastLog + 10000){
                    lastLog = now;
                }
            }
            oldInProgress = currentInProgress;
            try {
                synchronized (this.tasksInProgress) {
                    if (tasksInProgress.get() != oldInProgress) break;
                    this.tasksInProgress.wait(100);
                }
            }catch (InterruptedException e){
                throw new InterruptedException("#" + id + ", interrupted." +
                        " currentNumberOfTask=" + currentInProgress);
            }
        }
    }

    protected final BatchErrors globalErrors;
    //used for store all exception returned by query.
    protected static class BatchErrors {
        private final List<Throwable> throwables = new ArrayList<>();
        private final List<Row> actions = new ArrayList<>();
        private final List<String> addresses = new ArrayList<>();

        public synchronized void add(Throwable ex, Row row, ServerName serverName){
            if (row == null){
                throw new IllegalArgumentException("row cannot be null. location =" +serverName);
            }

            throwables.add(ex);
            actions.add(row);
            addresses.add(serverName != null ? serverName.toString() : "null");
        }

        public boolean hasErros() {return !throwables.isEmpty();}

        private synchronized RetriesExhaustedWithDetailsException makeException() {
            return  new RetriesExhaustedWithDetailsException(new ArrayList<Throwable>(throwables),
                    new ArrayList<Row>(actions), new ArrayList<String>(addresses));
        }

        public synchronized void clear() {
            throwables.clear();
            actions.clear();
            addresses.clear();
        }

        public synchronized void merge(BatchErrors other) {
            throwables.addAll(other.throwables);
            actions.addAll(other.actions);
            addresses.addAll(other.addresses);
        }
    }

    public static interface AsyncRequestFuture{
        public boolean hasError();
        public RetriesExhaustedException gerErrors();
        public List<? extends Rows> getFailedOperations();
        public Object[] getResults() throws InterruptedException;

        public void waitUntilDone() throws InterruptedException;
    }

    protected class AsyncRequestFutureImpl<CResult> implements AsyncRequestFuture {

        private final BatchErrors errors;
        private final ExecutorService pool;

        private final TableName tableName;

        public AsyncRequestFutureImpl(
                TableName tableName, List<Action<Row>> actions, long nonceGroup, ExecutorService pool,
                boolean needResults, Object[] results, Object callback) {
            this.errors = (globalErrors != null) ? globalErrors : new BatchErrors();
            this.pool = pool;
            this.tableName = tableName;
        }

        @Override
        public boolean hasError() {
            return false;
        }

        @Override
        public RetriesExhaustedException gerErrors() {
            return null;
        }

        @Override
        public List<? extends Rows> getFailedOperations() {
            return null;
        }

        @Override
        public Object[] getResults() throws InterruptedException {
            return new Object[0];
        }

        @Override
        public void waitUntilDone() throws InterruptedException {

        }

        /**
         * Group a list of acitons per region servers, and send them.
         *
         * @param currentActions
         * @param numAttempt
         */
        public void groupAndSendMultiAction(List<Action<Row>> currentActions, int numAttempt) {
            Map<ServerName, MultiAction<Row>> actionsByServer =
                    new HashMap<>();

            boolean isReplic = false;
            List<Action<Row>> unknownReplicaActions = null;

            for (Action<Row> action : currentActions) {
                RegionLocations locs = findAllLocationsOrFail(action, true);

                if (locs == null) continue;
                boolean isReplicaAction = !RegionReplicaUtil.isDefaultReplica(action.getReplicaId());
                if (isReplic && !isReplicaAction) {
                    throw new AssertionError("Replica and non-replica actions in the same retry.");
                }
                isReplic = isReplicaAction;

                HRegionLocation loc = locs.getRegionLocation(action.getReplicaId());
                if (loc == null || loc.getServerName() == null) {
                    if (isReplic){
                        if (unknownReplicaActions == null) {
                            unknownReplicaActions = new ArrayList<Action<Row>>();
                        }
                        unknownReplicaActions.add(action);
                    }
                    else {
                        manageLocationError(action, null);
                    }
                }
            }

        }

        private void manageLocationError(Action<Row> action, Object o) {
            String msg = "Cannot get replica " + action.getReplicaId()
                    + " location for " + action.getAction();

        }

        private RegionLocations findAllLocationsOrFail(Action<Row> action, boolean useCache) {
            if (action.getAction() == null) throw new IllegalArgumentException("");

            RegionLocations loc = null;

            try {
                loc = connection.locateRegion(
                        tableName, action.getAction().getRow(), useCache, true, action.getReplicaId());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return loc;
        }
    }


}
