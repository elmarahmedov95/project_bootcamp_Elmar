[train@DESKTOP-NFP7HPV project_bootcamp_Elmar]$ cd 06_spark
bash: cd: 06_spark: No such file or directory
[train@DESKTOP-NFP7HPV project_bootcamp_Elmar]$ cd 01_infra_layer
[train@DESKTOP-NFP7HPV 01_infra_layer]$ cd 06_spark
[train@DESKTOP-NFP7HPV 06_spark]$ helm repo add \
> spark-operator \
> https://kubeflow.github.io/spark-operator
"spark-operator" already exists with the same configuration, skipping
[train@DESKTOP-NFP7HPV 06_spark]$ helm repo update
Hang tight while we grab the latest from your chart repositories...
...Successfully got an update from the "rustfs" chart repository
...Successfully got an update from the "spark-operator" chart repository
...Successfully got an update from the "trino" chart repository
...Successfully got an update from the "superset" chart repository
...Successfully got an update from the "nessie-helm" chart repository
...Successfully got an update from the "cnpg" chart repository
Update Complete. ⎈Happy Helming!⎈
[train@DESKTOP-NFP7HPV 06_spark]$ kubectl create ns spark-operator
namespace/spark-operator created
[train@DESKTOP-NFP7HPV 06_spark]$ helm install \
> my-spark-operator \
> spark-operator/spark-operator \
> --namespace spark-operator \
> --set webhook.enable=true
NAME: my-spark-operator
LAST DEPLOYED: Wed Mar  4 10:09:12 2026
NAMESPACE: spark-operator
STATUS: deployed
REVISION: 1
TEST SUITE: None
[train@DESKTOP-NFP7HPV 06_spark]$ kubectl get all -n spark-operator
NAME                                                READY   STATUS    RESTARTS   AGE
pod/my-spark-operator-controller-5bd8699c49-rwc7l   1/1     Running   0          6m15s
pod/my-spark-operator-webhook-6699c597f9-khdhd      1/1     Running   0          6m15s

NAME                                    TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)    AGE
service/my-spark-operator-webhook-svc   ClusterIP   10.110.106.84   <none>        9443/TCP   6m15s

NAME                                           READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/my-spark-operator-controller   1/1     1            1           6m15s
deployment.apps/my-spark-operator-webhook      1/1     1            1           6m15s

NAME                                                      DESIRED   CURRENT   READY   AGE
replicaset.apps/my-spark-operator-controller-5bd8699c49   1         1         1       6m15s
replicaset.apps/my-spark-operator-webhook-6699c597f9      1         1         1       6m15s
[train@DESKTOP-NFP7HPV 06_spark]$ kubectl create serviceaccount spark
serviceaccount/spark created
[train@DESKTOP-NFP7HPV 06_spark]$ kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
clusterrolebinding.rbac.authorization.k8s.io/spark-role created
[train@DESKTOP-NFP7HPV 06_spark]$ kubectl get pods -n spark-operator
NAME                                            READY   STATUS    RESTARTS   AGE
my-spark-operator-controller-5bd8699c49-rwc7l   1/1     Running   0          8m46s
my-spark-operator-webhook-6699c597f9-khdhd      1/1     Running   0          8m46s
[train@DESKTOP-NFP7HPV 06_spark]$ kubectl get serviceaccount spark
NAME    SECRETS   AGE
spark   0         2m9s
[train@DESKTOP-NFP7HPV 02_submit_simple_sparkapp]$ docker build -t spark-k8s-app:2.0 .
[+] Building 187.8s (11/11) FINISHED                                                                                                                                                                                                                                                      docker:default
 => [internal] load build definition from Dockerfile                                                                                                                                                                                                                                                0.1s
 => => transferring dockerfile: 178B                                                                                                                                                                                                                                                                0.0s
 => [internal] load metadata for docker.io/library/spark:3.5.3-java17                                                                                                                                                                                                                               5.4s
 => [auth] library/spark:pull token for registry-1.docker.io                                                                                                                                                                                                                                        0.0s
 => [internal] load .dockerignore                                                                                                                                                                                                                                                                   0.0s
 => => transferring context: 2B                                                                                                                                                                                                                                                                     0.0s
 => [1/5] FROM docker.io/library/spark:3.5.3-java17@sha256:3d6e110ff627cd7bbf4dd10051acb3c7de6695b55cf174d20dc4b445f7f83bcd                                                                                                                                                                       139.6s
 => => resolve docker.io/library/spark:3.5.3-java17@sha256:3d6e110ff627cd7bbf4dd10051acb3c7de6695b55cf174d20dc4b445f7f83bcd                                                                                                                                                                         0.0s
 => => sha256:503f44d8615f8bd0b84ff8a6cf59f1a94eb81e0e238e58c0a7ad58e6e5ebc7e5 2.13kB / 2.13kB                                                                                                                                                                                                      0.7s
 => => sha256:4e7df7b51b99372cfe6991439fa0fa858f304f6c2b20f185a37291b91a5105b8 113.74MB / 113.74MB                                                                                                                                                                                                 88.4s
 => => sha256:2a8f05fe0dc23ec20ba4bf231425c7829029ee1114aa0d1dcd154dc179d90d31 324.87MB / 324.87MB                                                                                                                                                                                                125.6s
 => => sha256:5e92f08bf0cdbd518ee041853e677449065c3470655eab6528552f4c2562b846 21.66MB / 21.66MB                                                                                                                                                                                                   44.2s
 => => sha256:06415ef40bfbfb3fd983c8090a3b083213a7891e9322d1c6ed62e27a2f25b536 1.43kB / 1.43kB                                                                                                                                                                                                      1.1s
 => => sha256:1c99e6e516a5474bebeee0593966c60a8c43c6b8abc8ab2e651bd4ae22aae858 2.28kB / 2.28kB                                                                                                                                                                                                      0.8s
 => => sha256:2023aaf73bd8ccd243f2dfd135629182686997eb072b9ca65be21fd86ffbb376 158B / 158B                                                                                                                                                                                                          1.3s
 => => sha256:bf50b48fa6693be87391ed004de58b5f295b8ea2db307c2f43a422b60c294584 144.54MB / 144.54MB                                                                                                                                                                                                 96.7s
 => => sha256:99d0251816371836a858f54c5970ac350864e14f3750cae6c585f5c5db16839f 20.69MB / 20.69MB                                                                                                                                                                                                   11.1s
 => => sha256:6414378b647780fee8fd903ddb9541d134a1947ce092d08bdeb23a54cb3684ac 29.54MB / 29.54MB                                                                                                                                                                                                   12.0s
 => => extracting sha256:6414378b647780fee8fd903ddb9541d134a1947ce092d08bdeb23a54cb3684ac                                                                                                                                                                                                           3.3s
 => => extracting sha256:99d0251816371836a858f54c5970ac350864e14f3750cae6c585f5c5db16839f                                                                                                                                                                                                           2.5s
 => => extracting sha256:bf50b48fa6693be87391ed004de58b5f295b8ea2db307c2f43a422b60c294584                                                                                                                                                                                                           6.4s
 => => extracting sha256:2023aaf73bd8ccd243f2dfd135629182686997eb072b9ca65be21fd86ffbb376                                                                                                                                                                                                           0.1s
 => => extracting sha256:1c99e6e516a5474bebeee0593966c60a8c43c6b8abc8ab2e651bd4ae22aae858                                                                                                                                                                                                           0.1s
 => => extracting sha256:06415ef40bfbfb3fd983c8090a3b083213a7891e9322d1c6ed62e27a2f25b536                                                                                                                                                                                                           0.1s
 => => extracting sha256:5e92f08bf0cdbd518ee041853e677449065c3470655eab6528552f4c2562b846                                                                                                                                                                                                           1.4s
 => => extracting sha256:2a8f05fe0dc23ec20ba4bf231425c7829029ee1114aa0d1dcd154dc179d90d31                                                                                                                                                                                                           5.8s
 => => extracting sha256:503f44d8615f8bd0b84ff8a6cf59f1a94eb81e0e238e58c0a7ad58e6e5ebc7e5                                                                                                                                                                                                           0.0s
 => => extracting sha256:4f4fb700ef54461cfa02571ae0db9a0dc1e0cdb5577484a6d75e68dc38e8acc1                                                                                                                                                                                                           0.0s
 => => extracting sha256:4e7df7b51b99372cfe6991439fa0fa858f304f6c2b20f185a37291b91a5105b8                                                                                                                                                                                                           7.7s
 => [internal] load build context                                                                                                                                                                                                                                                                   0.0s
 => => transferring context: 557B                                                                                                                                                                                                                                                                   0.0s
 => [2/5] WORKDIR /app                                                                                                                                                                                                                                                                              0.3s
 => [3/5] COPY requirements.txt .                                                                                                                                                                                                                                                                   0.3s
 => [4/5] RUN pip3 install -r requirements.txt                                                                                                                                                                                                                                                     28.8s
 => [5/5] COPY spark_on_k8s_app.py .                                                                                                                                                                                                                                                                0.1s
 => exporting to image                                                                                                                                                                                                                                                                             12.7s
 => => exporting layers                                                                                                                                                                                                                                                                             9.9s
 => => exporting manifest sha256:f2c44e9557c918a517a053515742cfdab3fd2e346daef83b35e603a7900fd5fa                                                                                                                                                                                                   0.0s
 => => exporting config sha256:d65b4c014def1c22c11d32d28e288bb4608f57ba9315b1a1d8c4f4089c269ff0                                                                                                                                                                                                     0.0s
 => => exporting attestation manifest sha256:a2556c0c57d9fd4a0fe59b9ac8defc9760906ccb64c3bbd9ec090a411c44897b                                                                                                                                                                                       0.0s
 => => exporting manifest list sha256:001604ce1f5651c63fb2c2339ee878f602c87ecadb34d0c5379f5b57615f3659                                                                                                                                                                                              0.0s
 => => naming to docker.io/library/spark-k8s-app:2.0                                                                                                                                                                                                                                                0.0s
 => => unpacking to docker.io/library/spark-k8s-app:2.0                                                                                                                                                                                                                                             2.6s
[train@DESKTOP-NFP7HPV 02_submit_simple_sparkapp]$ docker images | grep spark-k8s-app
WARNING: This output is designed for human readability. For machine-readable output, please use --format.
spark-k8s-app:2.0                                                                                       001604ce1f56        2.1GB          725MB        
[train@DESKTOP-NFP7HPV 02_submit_simple_sparkapp]$ kubectl apply -f sparkApplication.yaml
sparkapplication.sparkoperator.k8s.io/pyspark-on-k8s created
[train@DESKTOP-NFP7HPV 02_submit_simple_sparkapp]$ kubectl get pods -w
NAME                    READY   STATUS    RESTARTS   AGE
pyspark-on-k8s-driver   1/1     Running   0          6s
pyspark-on-k8s-exec-1   0/1     Pending   0          0s
pyspark-on-k8s-exec-1   0/1     Pending   0          0s
pyspark-on-k8s-exec-1   0/1     ContainerCreating   0          0s
pyspark-on-k8s-exec-2   0/1     Pending             0          0s
pyspark-on-k8s-exec-2   0/1     Pending             0          0s
pyspark-on-k8s-exec-2   0/1     ContainerCreating   0          0s
pyspark-on-k8s-exec-1   1/1     Running             0          2s
pyspark-on-k8s-exec-2   1/1     Running             0          4s
pyspark-on-k8s-exec-1   1/1     Terminating         0          54s
pyspark-on-k8s-exec-2   1/1     Terminating         0          55s
pyspark-on-k8s-exec-1   1/1     Terminating         0          55s
pyspark-on-k8s-exec-2   1/1     Terminating         0          55s
pyspark-on-k8s-exec-2   0/1     Completed           0          57s
pyspark-on-k8s-exec-1   0/1     Completed           0          57s
pyspark-on-k8s-driver   0/1     Completed           0          71s
pyspark-on-k8s-exec-1   0/1     Completed           0          57s
pyspark-on-k8s-exec-1   0/1     Completed           0          57s
pyspark-on-k8s-exec-2   0/1     Completed           0          58s
pyspark-on-k8s-exec-2   0/1     Completed           0          58s
pyspark-on-k8s-driver   0/1     Completed           0          73s
pyspark-on-k8s-driver   0/1     Completed           0          6m11s
pyspark-on-k8s-driver   0/1     Completed           0          6m11s

# Loglarda neticeler
[train@DESKTOP-NFP7HPV project_bootcamp_Elmar]$ kubectl logs -f pyspark-on-k8s-driver
26/03/04 07:33:40 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
26/03/04 07:33:40 INFO SparkContext: Running Spark version 3.5.3
26/03/04 07:33:40 INFO SparkContext: OS info Linux, 6.6.87.2-microsoft-standard-WSL2, amd64
26/03/04 07:33:40 INFO SparkContext: Java version 17.0.13
26/03/04 07:33:41 INFO ResourceUtils: ==============================================================
26/03/04 07:33:41 INFO ResourceUtils: No custom resources configured for spark.driver.
26/03/04 07:33:41 INFO ResourceUtils: ==============================================================
26/03/04 07:33:41 INFO SparkContext: Submitted application: Spark on K8s
26/03/04 07:33:41 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1500, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
26/03/04 07:33:41 INFO ResourceProfile: Limiting resource is cpus at 1 tasks per executor
26/03/04 07:33:41 INFO ResourceProfileManager: Added ResourceProfile id: 0
26/03/04 07:33:41 INFO SecurityManager: Changing view acls to: spark
26/03/04 07:33:41 INFO SecurityManager: Changing modify acls to: spark
26/03/04 07:33:41 INFO SecurityManager: Changing view acls groups to: 
26/03/04 07:33:41 INFO SecurityManager: Changing modify acls groups to: 
26/03/04 07:33:41 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: spark; groups with view permissions: EMPTY; users with modify permissions: spark; groups with modify permissions: EMPTY
26/03/04 07:33:41 INFO Utils: Successfully started service 'sparkDriver' on port 7078.
26/03/04 07:33:41 INFO SparkEnv: Registering MapOutputTracker
26/03/04 07:33:41 INFO SparkEnv: Registering BlockManagerMaster
26/03/04 07:33:41 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
26/03/04 07:33:41 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
26/03/04 07:33:41 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
26/03/04 07:33:41 INFO DiskBlockManager: Created local directory at /var/data/spark-348ca257-7056-4967-b917-215db6000d6d/blockmgr-4f2d9902-0152-47c5-bf7a-913650f1a00c
26/03/04 07:33:41 INFO MemoryStore: MemoryStore started with capacity 117.0 MiB
26/03/04 07:33:41 INFO SparkEnv: Registering OutputCommitCoordinator
26/03/04 07:33:42 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
26/03/04 07:33:42 INFO Utils: Successfully started service 'SparkUI' on port 4040.
26/03/04 07:33:42 INFO SparkKubernetesClientFactory: Auto-configuring K8S client using current context from users K8S config file
26/03/04 07:33:46 INFO ExecutorPodsAllocator: Going to request 2 executors from Kubernetes for ResourceProfile Id: 0, target: 2, known: 0, sharedSlotFromPendingPods: 2147483647.
26/03/04 07:33:47 INFO ExecutorPodsAllocator: Found 0 reusable PVCs from 0 PVCs
26/03/04 07:33:47 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 7079.
26/03/04 07:33:47 INFO NettyBlockTransferService: Server created on pyspark-on-k8s-bbacab9cb7c425f0-driver-svc.default.svc 10.1.0.136:7079
26/03/04 07:33:47 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
26/03/04 07:33:47 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, pyspark-on-k8s-bbacab9cb7c425f0-driver-svc.default.svc, 7079, None)
26/03/04 07:33:47 INFO BlockManagerMasterEndpoint: Registering block manager pyspark-on-k8s-bbacab9cb7c425f0-driver-svc.default.svc:7079 with 117.0 MiB RAM, BlockManagerId(driver, pyspark-on-k8s-bbacab9cb7c425f0-driver-svc.default.svc, 7079, None)
26/03/04 07:33:47 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, pyspark-on-k8s-bbacab9cb7c425f0-driver-svc.default.svc, 7079, None)
26/03/04 07:33:47 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, pyspark-on-k8s-bbacab9cb7c425f0-driver-svc.default.svc, 7079, None)
26/03/04 07:33:47 INFO BasicExecutorFeatureStep: Decommissioning not enabled, skipping shutdown script
26/03/04 07:33:47 INFO BasicExecutorFeatureStep: Decommissioning not enabled, skipping shutdown script
26/03/04 07:33:58 INFO KubernetesClusterSchedulerBackend$KubernetesDriverEndpoint: No executor found for 10.1.0.137:42738
26/03/04 07:33:58 INFO KubernetesClusterSchedulerBackend$KubernetesDriverEndpoint: No executor found for 10.1.0.138:55676
26/03/04 07:33:59 INFO KubernetesClusterSchedulerBackend$KubernetesDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (10.1.0.138:55682) with ID 2,  ResourceProfileId 0
26/03/04 07:33:59 INFO KubernetesClusterSchedulerBackend$KubernetesDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (10.1.0.137:42746) with ID 1,  ResourceProfileId 0
26/03/04 07:33:59 INFO KubernetesClusterSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.8
26/03/04 07:34:00 INFO BlockManagerMasterEndpoint: Registering block manager 10.1.0.138:40163 with 720.0 MiB RAM, BlockManagerId(2, 10.1.0.138, 40163, None)
26/03/04 07:34:00 INFO BlockManagerMasterEndpoint: Registering block manager 10.1.0.137:41897 with 720.0 MiB RAM, BlockManagerId(1, 10.1.0.137, 41897, None)
26/03/04 07:34:04 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
26/03/04 07:34:04 INFO SharedState: Warehouse path is 'file:/app/spark-warehouse'.
*******************************
root
 |-- id: long (nullable = false)
 |-- plus_10: long (nullable = false)
 |-- plus_20: long (nullable = false)

26/03/04 07:34:08 INFO CodeGenerator: Code generated in 347.818514 ms
26/03/04 07:34:09 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
26/03/04 07:34:09 INFO DAGScheduler: Got job 0 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/03/04 07:34:09 INFO DAGScheduler: Final stage: ResultStage 0 (showString at NativeMethodAccessorImpl.java:0)
26/03/04 07:34:09 INFO DAGScheduler: Parents of final stage: List()
26/03/04 07:34:09 INFO DAGScheduler: Missing parents: List()
26/03/04 07:34:09 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[3] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/03/04 07:34:09 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 11.8 KiB, free 117.0 MiB)
26/03/04 07:34:09 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 5.3 KiB, free 116.9 MiB)
26/03/04 07:34:09 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on pyspark-on-k8s-bbacab9cb7c425f0-driver-svc.default.svc:7079 (size: 5.3 KiB, free: 117.0 MiB)
26/03/04 07:34:09 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1585
26/03/04 07:34:09 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[3] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/03/04 07:34:09 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
26/03/04 07:34:09 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (10.1.0.138, executor 2, partition 0, PROCESS_LOCAL, 9143 bytes) 
26/03/04 07:34:10 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 10.1.0.138:40163 (size: 5.3 KiB, free: 720.0 MiB)
26/03/04 07:34:11 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1235 ms on 10.1.0.138 (executor 2) (1/1)
26/03/04 07:34:11 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
26/03/04 07:34:11 INFO DAGScheduler: ResultStage 0 (showString at NativeMethodAccessorImpl.java:0) finished in 1.721 s
26/03/04 07:34:11 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
26/03/04 07:34:11 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
26/03/04 07:34:11 INFO DAGScheduler: Job 0 finished: showString at NativeMethodAccessorImpl.java:0, took 1.880900 s
26/03/04 07:34:11 INFO CodeGenerator: Code generated in 69.096532 ms
+---+-------+-------+
| id|plus_10|plus_20|
+---+-------+-------+
|  0|     10|     20|
|  1|     11|     21|
|  2|     12|     22|
|  3|     13|     23|
|  4|     14|     24|
|  5|     15|     25|
|  6|     16|     26|
|  7|     17|     27|
|  8|     18|     28|
|  9|     19|     29|
| 10|     20|     30|
| 11|     21|     31|
| 12|     22|     32|
| 13|     23|     33|
| 14|     24|     34|
| 15|     25|     35|
| 16|     26|     36|
| 17|     27|     37|
| 18|     28|     38|
| 19|     29|     39|
| 20|     30|     40|
| 21|     31|     41|
| 22|     32|     42|
| 23|     33|     43|
| 24|     34|     44|
| 25|     35|     45|
| 26|     36|     46|
| 27|     37|     47|
| 28|     38|     48|
| 29|     39|     49|
| 30|     40|     50|
| 31|     41|     51|
| 32|     42|     52|
| 33|     43|     53|
| 34|     44|     54|
| 35|     45|     55|
| 36|     46|     56|
| 37|     47|     57|
| 38|     48|     58|
| 39|     49|     59|
| 40|     50|     60|
| 41|     51|     61|
| 42|     52|     62|
| 43|     53|     63|
| 44|     54|     64|
| 45|     55|     65|
| 46|     56|     66|
| 47|     57|     67|
| 48|     58|     68|
| 49|     59|     69|
| 50|     60|     70|
| 51|     61|     71|
| 52|     62|     72|
| 53|     63|     73|
| 54|     64|     74|
| 55|     65|     75|
| 56|     66|     76|
| 57|     67|     77|
| 58|     68|     78|
| 59|     69|     79|
| 60|     70|     80|
| 61|     71|     81|
| 62|     72|     82|
| 63|     73|     83|
| 64|     74|     84|
| 65|     75|     85|
| 66|     76|     86|
| 67|     77|     87|
| 68|     78|     88|
| 69|     79|     89|
| 70|     80|     90|
| 71|     81|     91|
| 72|     82|     92|
| 73|     83|     93|
| 74|     84|     94|
| 75|     85|     95|
| 76|     86|     96|
| 77|     87|     97|
| 78|     88|     98|
| 79|     89|     99|
| 80|     90|    100|
| 81|     91|    101|
| 82|     92|    102|
| 83|     93|    103|
| 84|     94|    104|
| 85|     95|    105|
| 86|     96|    106|
| 87|     97|    107|
| 88|     98|    108|
| 89|     99|    109|
| 90|    100|    110|
| 91|    101|    111|
| 92|    102|    112|
| 93|    103|    113|
| 94|    104|    114|
| 95|    105|    115|
| 96|    106|    116|
| 97|    107|    117|
| 98|    108|    118|
| 99|    109|    119|
+---+-------+-------+
only showing top 100 rows

Spark is shutting down....
26/03/04 07:34:41 INFO SparkContext: SparkContext is stopping with exitCode 0.
26/03/04 07:34:41 INFO SparkUI: Stopped Spark web UI at http://pyspark-on-k8s-bbacab9cb7c425f0-driver-svc.default.svc:4040
26/03/04 07:34:41 INFO KubernetesClusterSchedulerBackend: Shutting down all executors
26/03/04 07:34:41 INFO KubernetesClusterSchedulerBackend$KubernetesDriverEndpoint: Asking each executor to shut down
26/03/04 07:34:41 WARN ExecutorPodsWatchSnapshotSource: Kubernetes client has been closed.
26/03/04 07:34:42 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
26/03/04 07:34:42 INFO MemoryStore: MemoryStore cleared
26/03/04 07:34:42 INFO BlockManager: BlockManager stopped
26/03/04 07:34:42 INFO BlockManagerMaster: BlockManagerMaster stopped
26/03/04 07:34:42 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
26/03/04 07:34:42 INFO SparkContext: Successfully stopped SparkContext
26/03/04 07:34:43 INFO ShutdownHookManager: Shutdown hook called
26/03/04 07:34:43 INFO ShutdownHookManager: Deleting directory /tmp/spark-c12c64fc-c2a7-4720-b099-d8983592d41b
26/03/04 07:34:43 INFO ShutdownHookManager: Deleting directory /tmp/spark-948bb9be-ad41-4e80-9721-5abcef23f2dd
26/03/04 07:34:43 INFO ShutdownHookManager: Deleting directory /var/data/spark-348ca257-7056-4967-b917-215db6000d6d/spark-1d37d77c-cd5a-4886-a253-a362e4160574/pyspark-faa65378-00d8-4452-b865-c058f275a357
26/03/04 07:34:43 INFO ShutdownHookManager: Deleting directory /var/data/spark-348ca257-7056-4967-b917-215db6000d6d/spark-1d37d77c-cd5a-4886-a253-a362e4160574
26/03/04 07:34:43 INFO ShutdownHookManager: Deleting directory /tmp/spark-d05d4276-fe87-47d7-a036-3a6fc339eab7
26/03/04 07:34:43 INFO ShutdownHookManager: Deleting directory /tmp/spark-4e01bfa3-25cf-4dad-b6f5-aa443f662738