package org.apache.hadoop.hbase.client;

import com.sun.net.httpserver.Authenticator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.Server;
import org.apache.htrace.Trace;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jh4j on 2016/9/8.
 */
public class AsyncProcess {

    protected static final Log LOG = LogFactory.getLog(AsyncProcess.class);
    protected final ClusterConnection connection;
    protected int numTries;
    protected static final AtomicLong COUNTER = new AtomicLong();
    protected int serverTrackerTimeout;
    protected final long pause;



    public AsyncProcess(ClusterConnection hc, Configuration conf, ExecutorService pool,
                        RpcRetryingCallerFactory rpcCaller, boolean useGlobalErrors, RpcControllerFactory rpcFactory) {
        this.connection = hc;
        this.pool = pool;
        this.maxTotalConcurrentTasks = conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS);
        this.id = COUNTER.incrementAndGet();

        this.timeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
                HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

        this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
                HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

        this.globalErrors = useGlobalErrors ? new BatchErrors() : null;

        this.serverTrackerTimeout = 0 ;
        for (int i =0 ; i < this.numTries ; i ++) {
            serverTrackerTimeout += ConnectionUtils.getPauseTime(this.pause ,i);
        }
        this.rpcFactory = rpcFactory;
        this.rpcCallerFactory = rpcCaller;
    }

    /**
     * The number of tasks simultaneously executed on the cluster.
     */
    protected final int maxTotalConcurrentTasks;
    public <CResult> AsyncRequestFuture submitAll(ExecutorService pool, TableName tableName, List<? extends Row> rows, Batch.Callback<CResult> callback, Object[] results) throws InterruptedException {

        List<Action<Row>> actions = new ArrayList<>(rows.size());

        int posInList = -1;
        NonceGenerator ng = this.connection.getNonceGenerator();

        for (Row r : rows){
            posInList++;
//            if (r instanceof Put)
            Action<Row> action = new Action<Row>(r, posInList);
            setNonce(ng, r, action);
            actions.add(action);
        }

        AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(
                tableName, actions, ng.getNonceGroup(), getPool(pool), callback, results, results != null);

        ars.groupAndSendMultiAction(actions, 1);
        return ars;
    }

    private <CResult> AsyncRequestFutureImpl<CResult> createAsyncRequestFuture(
            TableName tableName, List<Action<Row>> actions, long nonceGroup, ExecutorService pool, Batch.Callback<CResult> callback,
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
        private final long nonceGroup;
        private final boolean hasAnyReplicaGets;

        private final AtomicLong actionsInProgress = new AtomicLong(-1);

        private final Object[] results;
        private final int[] replicaGetIndices;

        private final ConnectionManager.ServerErrorTracker errorsByServer;

        public AsyncRequestFutureImpl(
                TableName tableName, List<Action<Row>> actions, long nonceGroup, ExecutorService pool,
                boolean needResults, Object[] results, Batch.Callback<CResult> callback) {
            this.pool = pool;
            this.tableName = tableName;
            this.nonceGroup = nonceGroup;
            this.actionsInProgress.set(actions.size());
            this.callback = callback;
            if (results != null) {
                assert needResults;
                if (results.length != actions.size()) throw new AssertionError("results.length");
                this.results = results;
                for (int i = 0 ; i != this.results.length ; ++i) {
                    results[i] = null;
                }
            }else {
                this.results = needResults ? new Object[actions.size()] : null;
            }
            List<Integer> replicaGetIndices = null;

            boolean hasAnyReplicaGets = false;
            if (needResults) {
                boolean hasAnyNonReplicaReqs = false;
                int posInList = 0;
                for (Action<Row> action : actions){
                    boolean isReplicaGet = isReplicaGet(action.getAction());
                    if (isReplicaGet) {
                        hasAnyReplicaGets = true;
                        if (hasAnyNonReplicaReqs) {
                            if (replicaGetIndices == null){
                                replicaGetIndices = new ArrayList<>(actions.size() -1);
                            }
                            replicaGetIndices.add(posInList);
                        }
                    } else if (!hasAnyNonReplicaReqs) {
                        hasAnyNonReplicaReqs = true;
                        if (posInList > 0) {
                            replicaGetIndices = new ArrayList<>(actions.size() -1);
                            for (int i = 0; i < posInList ; i ++){
                                replicaGetIndices.add(i);
                            }
                        }
                    }
                    ++ posInList;
                }
            }
            this.hasAnyReplicaGets = hasAnyReplicaGets;
            if (replicaGetIndices != null){
                this.replicaGetIndices = new int[replicaGetIndices.size()];
                int i = 0;
                for (Integer el : replicaGetIndices){
                    this.replicaGetIndices[i++] = el;
                }
            } else {
                this.replicaGetIndices = null;
            }
            this.errorsByServer = createServerErrorTracker();
            this.errors = (globalErrors != null) ? globalErrors : new BatchErrors();
        }


        private boolean isReplicaGet(Row row) {

            return (row instanceof Get) && (((Get)row).getConsistency() == Consistency.TIMELINE);
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
                } else {
                    byte[] regionName = loc.getRegionInfo().getRegionName();
                    addAction(loc.getServerName(), regionName, action, actionsByServer, nonceGroup);
                }
            }

            boolean doStartReplica = (numAttempt == 1 && !isReplic && hasAnyReplicaGets);
            boolean hasUnknown = unknownReplicaActions != null && !unknownReplicaActions.isEmpty();

            if (!actionsByServer.isEmpty()) {
                sendMultiAction(actionsByServer, numAttempt, (doStartReplica && !hasUnknown) ? currentActions : null, numAttempt > 1 && !hasUnknown);
            }

        }

        private void sendMultiAction(Map<ServerName, MultiAction<Row>> actionsByServer, int numAttempt, List<Action<Row>> actionsForReplicaThread, boolean reuseThread) {
            int actionsRemaining = actionsByServer.size();

            for (Map.Entry<ServerName, MultiAction<Row>> e : actionsByServer.entrySet()) {
                ServerName server = e.getKey();
                MultiAction<Row> multiAction = e.getValue();
                incTaskCounters(multiAction.getRegions(), server);

                Collection<? extends Runnable> runnables = getNewMultiActionRunnable(server, multiAction, numAttempt);

                //make sure we correctly count the number of runnables before we try to reuse the send
                //thread , incase we had to split the request into different runnables because of backoff
                if (runnables.size() > actionsRemaining) {
                    actionsRemaining = runnables.size();
                }

                for (Runnable runnable : runnables) {
                    if ((--actionsRemaining == 0) && reuseThread) {
                        runnable.run();
                    }else {
                        try {
                            pool.submit(runnable);
                        }catch (RejectedExecutionException ree) {
                            decTaskCounters(multiAction.getRegions(), server);
                            LOG.warn("warning message.");
                        }
                    }
                }

            }

            if (actionsForReplicaThread != null) {
                startWaitingForReplicaCalls(actionsForReplicaThread);
            }

        }

        private void startWaitingForReplicaCalls(List<Action<Row>> actionsForReplicaThread) {
            long startTime = EnvironmentEdgeManager.currentTime();
//            AsyncRequestFutureImpl.ReplicaCallIssuingRunnable
        }

        private void decTaskCounters(Set<byte[]> regions, ServerName server) {
            for (byte[] regBytes : regions) {
                AtomicInteger regionCnt = taskCounterPerRegion.get(regBytes);
                regionCnt.decrementAndGet();
            }

            taskCounterPerServer.get(server).decrementAndGet();
            tasksInProgress.decrementAndGet();
            synchronized (tasksInProgress) {
                tasksInProgress.notifyAll();
            }
        }

        private Collection<? extends Runnable> getNewMultiActionRunnable(ServerName server, MultiAction<Row> multiAction, int numAttempt) {

            if (AsyncProcess.this.connection.getStatisticsTracker() == null){
                return Collections.singletonList(Trace.wrap("AsyncProcess.sendMultiAction", new SingleServerRequestRunnable(multiAction, numAttempt, server)));
            }

            Map<Long, DelayingRunner> actions = new HashMap<>(multiAction.size());

            for (Map.Entry<byte[], List<Action<Row>>> e : multiAction.actions.entrySet()) {
                Long backoff = getBackoff(server, e.getKey());
                DelayingRunner runner = actions.get(backoff);
                if (runner == null){
                    actions.put(backoff, new DelayingRunner(backoff,e));
                } else {
                    runner.add(e);
                }
            }

            List<Runnable> toReturn = new ArrayList<>(actions.size());
            for (DelayingRunner runner : actions.values()) {
                String traceText = "AsyncProcess.sendMultiAction";
                Runnable runnable =
                        new SingleServerRequestRunnable(runner.getActions(), numAttempt, server);

                if (runner.getSleepTime() > 0) {
                    runner.setRunner(runnable);
                    traceText = "AsyncProcess.clientBackoff.sendMultiAction";
                    runnable = runner;
                }
                runnable = Trace.wrap(traceText, runnable);
                toReturn.add(runnable);
            }
            return toReturn;
        }

        private Long getBackoff(ServerName server, byte[] regionName) {
            ServerStatisticTracker tracker = AsyncProcess.this.connection.getStatisticsTracker();
            ServerStatistics stats = tracker.getStats(server);
            return AsyncProcess.this.connection.getBackoffPolicy().getBackoffTime(server, regionName, stats);
        }

        protected final ConcurrentMap<byte[], AtomicInteger> taskCounterPerRegion =
                new ConcurrentSkipListMap<byte[], AtomicInteger>(Bytes.BYTES_COMPARATOR);
        protected final ConcurrentMap<ServerName, AtomicInteger> taskCounterPerServer =
                new ConcurrentHashMap<ServerName, AtomicInteger>();
        private void incTaskCounters(Set<byte[]> regions, ServerName serverName) {
            tasksInProgress.incrementAndGet();

            AtomicInteger serverCnt = taskCounterPerServer.get(serverName);
            if (serverCnt == null){
                taskCounterPerServer.put(serverName, new AtomicInteger());
                serverCnt = taskCounterPerServer.get(serverName);
            }
            serverCnt.incrementAndGet();

            for (byte[] regBytes : regions) {
                AtomicInteger regionCnt = taskCounterPerRegion.get(regBytes);
                if (regionCnt == null) {
                    regionCnt = new AtomicInteger();
                    AtomicInteger oldCnt = taskCounterPerRegion.putIfAbsent(regBytes, regionCnt);
                    if (oldCnt != null){
                        regionCnt = oldCnt;
                    }
                }
                regionCnt.incrementAndGet();
            }
        }

        private void addAction(ServerName serverName, byte[] regionName, Action<Row> action, Map<ServerName, MultiAction<Row>> actionsByServer, long nonceGroup) {
            MultiAction<Row> multiAction = actionsByServer.get(serverName);

            if (multiAction == null){
                multiAction = new MultiAction<>();
                actionsByServer.put(serverName, multiAction);
            }
            if (action.hasNonce() && !multiAction.hasNonceGroup()) {
                multiAction.setNonceGroup(nonceGroup);
            }

            multiAction.add(regionName, action);
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

        private final class SingleServerRequestRunnable implements Runnable {

            private final MultiAction<Row> multiAction;
            private final int numAttempt;
            private final ServerName server;

            public SingleServerRequestRunnable(MultiAction<Row> multiAction, int numAttempt, ServerName server) {
                this.multiAction = multiAction;
                this.numAttempt = numAttempt;
                this.server = server;
            }

            @Override
            public void run() {
                MultiResponse res;

                MultiServerCallable<Row> callable = createCallable(server, tableName, multiAction);
                try {
                    res = createCaller(callable).callWithoutRetries(callable, timeout);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                receiveMultiAction(multiAction, server, res, numAttempt);
            }
        }

        private final Batch.Callback<CResult> callback;
        private void receiveMultiAction(MultiAction<Row> multiAction, ServerName server, MultiResponse res, int numAttempt) {
            assert res != null;

            List<Action<Row>> toReplay = new ArrayList<>();
            Throwable throwable = null;

            int failureCount = 0;
            boolean canRetry = true;

            int failed = 0 , stopped = 0;
            for (Map.Entry<byte[], List<Action<Row>>> regionEntry : multiAction.actions.entrySet()) {
                byte[] regionName = regionEntry.getKey();
                Map<Integer, Object> regionResults = res.getResults().get(regionName);
                if (regionResults  == null) {
                    if (!res.getExceptions().containsKey(regionName)) {
                        LOG.error("");
                        res.getExceptions().put(regionName, new RuntimeException(""));
                    }
                    continue;
                }
                boolean regionFailureRegistered = false;

                for (Action<Row> sentAction : regionEntry.getValue()) {
                    Object result = regionResults.get(sentAction.getOriginalIndex());

                    if (result == null || result instanceof Throwable) {
                        Row row = sentAction.getAction();

                        if (!regionFailureRegistered) {
                            regionFailureRegistered = true;
                            connection.updateCachedLocations(
                                tableName, regionName, row.getRow(), result, server);
                        }
                        if (failureCount == 0){
                            errorsByServer.reportServerError(server);
                            //We determine canRetry only once for all calls, after reporting server failure.
                            canRetry = errorsByServer.canRetryMore(numAttempt);
                        }
                        //some retry action
                    } else {
                        if (AsyncProcess.this.connection.getStatisticsTracker() != null){
                            result = ResultStatsUtil.updateStats(result,
                                    AsyncProcess.this.connection.getStatisticsTracker(), server, regionName);
                        }
                        if (callback != null) {
                            this.callback.update(regionName, sentAction.getAction().getRow(), (CResult)result);
                        }
                        setResult(sentAction, result);
                    }
                }
            }
        }

        private void setResult(Action<Row> sentAction, Object result) {
            if (result == null){
                throw new RuntimeException("Result cannot be null");
            }

            ReplicaResultState state = null;

            boolean isStale = !RegionReplicaUtil.isDefaultReplica(sentAction.getReplicaId());

            int index = sentAction.getOriginalIndex();
            if (results == null) {
                decActionCounter(index);
                return;
            } else if ((state = trySetResultSimple(index, sentAction.getAction(), false, result, null, isStale)) == null) {
                return;
            }

            assert state != null;

            synchronized (state) {
                if (state.callCount == 0) return;
                state.callCount = 0;
            }

        }

        private final Object replicaResultLock = new Object();
        private ReplicaResultState trySetResultSimple(int index, Row row, boolean isError, Object result, ServerName server, boolean isFromReplica) {

            Object resObj = null;
            if (!isReplicaGet(row)) {
                if (isFromReplica) {
                    throw new AssertionError("");
                }
                results[index] = result;
            } else {
                synchronized (replicaResultLock) {
                    if ((resObj = results[index]) == null) {
                        if (isFromReplica) {
                            throw new AssertionError("");
                        }
                        results[index] = result;
                    }
                }
            }
            //ToDo
            ReplicaResultState rrs =
                    (resObj instanceof ReplicaResultState) ? (ReplicaResultState)resObj : null;

            if (rrs == null && isError) {
                errors.add((Throwable)result, row, server);
            }

            if (resObj == null) {
                decActionCounter(index);
                return null;
            }
            return rrs;
        }

        private void decActionCounter(int index) {
            long actionRemaining = actionsInProgress.decrementAndGet();
            if (actionRemaining < 0) {
                String error = buildDetailedErrorMsg("Incorrect actions in progress", index);
                throw new AssertionError(error);
            }else if (actionRemaining == 0) {
                synchronized (actionsInProgress) {
                    actionsInProgress.notifyAll();
                }
            }
        }

        private String buildDetailedErrorMsg(String string, int index) {
            String error = string + "; called for " + index +
                    ", actionsInProgress " + actionsInProgress.get() + "; replica gets: ";
            if (replicaGetIndices != null) {
                for (int i = 0; i < replicaGetIndices.length; ++i) {
                    error += replicaGetIndices[i] + ", ";
                }
            } else {
                error += (hasAnyReplicaGets ? "all" : "none");
            }
            error += "; results ";
            if (results != null) {
                for (int i = 0; i < results.length; ++i) {
                    Object o = results[i];
                    error += ((o == null) ? "null" : o.toString()) + ", ";
                }
            }
            return error;
        }
    }

    protected int timeout;

    protected final RpcRetryingCallerFactory rpcCallerFactory;
    private RpcRetryingCaller<MultiResponse> createCaller(MultiServerCallable<Row> callable) {
        return rpcCallerFactory.<MultiResponse>newCaller();
    }

    protected final RpcControllerFactory rpcFactory;
    private MultiServerCallable<Row> createCallable(ServerName server, TableName tableName, MultiAction<Row> multiAction) {
        return new MultiServerCallable<>(connection, tableName, server, this.rpcFactory, multiAction);
    }

    protected ConnectionManager.ServerErrorTracker createServerErrorTracker() {
        return new ConnectionManager.ServerErrorTracker(this.serverTrackerTimeout, this.numTries);
    }

    private static class ReplicaResultState {
        /**Number of calls outstanding, or 0 if a call succeeded(even with others outstanding). */
        int callCount;

        public ReplicaResultState(int callCount) {
            this.callCount = callCount;
        }

        BatchErrors replicaErrors = null;

        @Override
        public String toString() {
            return "ReplicaResultState{" +
                    "callCount=" + callCount +
                    ", replicaErrors=" + replicaErrors +
                    '}';
        }
    }

}
