---
title: "配置参数"
weight: 3
type: docs
aliases:
  - /zh/deployment/config.html
  - /zh/ops/config.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 配置参数

所有配置都在 `confflink-conf.yaml` 中完成，它应该是一个 [YAML 键值对](http://www.yaml.org/spec/1.2/spec.html) 格式为 `key: value` 的集合。

在 Flink 进程启动时解析和评估配置。对配置文件的更改需要重新启动相关进程。

开箱即用的配置将使用您的默认 Java 安装。如果您想手动覆盖 Java 运行时以使用，您可以在 `/conf/flink-conf.yaml` 中手动设置环境变量 `JAVA_HOME` 或配置键 `env.java.home`。

您可以通过定义 `FLINK_CONF_DIR` 环境变量来指定不同的配置目录位置。对于提供非会话部署的资源提供者，您可以通过这种方式指定每个作业的配置。从 Flink 发行版复制 `conf` 目录，并在每个作业的基础上修改设置。请注意，这在 Docker 或独立的 Kubernetes 部署中不受支持。在基于 Docker 的部署中，您可以使用“FLINK_PROPERTIES”环境变量来传递配置值。

在会话集群上，提供的配置将仅用于配置 [execution](#execution) 参数，例如影响作业的配置参数，而不是底层集群。

# 基本设置

默认配置支持启动单节点 Flink 会话集群，无需任何更改。
本节中的配置项是分布式 Flink 设置最基础常用的配置项。

**主机名 / 端口**

这些配置项仅对 *standalone* 应用程序或会话部署（[simple standalone]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}) 或 [Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}))。

如果您将 Flink 与 [Yarn]({{< ref "docsdeploymentresource-providersyarn" >}}) 或 [*active* Kubernetes integration]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}) 一起使用，主机名和端口会自动发现。

- `rest.address`, `rest.port`: 这些被客户端用来连接到 Flink。将其设置为 JobManager 运行的主机名或 JobManager REST 接口前面的 (Kubernetes) 服务的主机名。

- TaskManager 使用 `jobmanager.rpc.address`（默认为 *"localhost"*）和 `jobmanager.rpc.port`（默认为 *6123*）配置项来连接到 JobManager/ResourceManager。将此设置为 JobManager 运行的主机名或 JobManager 的（Kubernetes 内部）服务的主机名。这个配置项在 [setups with high-availability]({{< ref "docs/deployment/ha/overview" >}}) 无效，这里是通过自动发现来选举主节点。

**内存大小**

默认内存设置支持简单的流批处理应用程序，但太小而无法为更复杂的应用程序提供良好的性能。

- `jobmanager.memory.process.size`: *JobManager* (JobMaster / ResourceManager / Dispatcher) 进程的总大小。
- `taskmanager.memory.process.size`: TaskManager 进程的总大小。

总大小包括一切。 Flink 会为 JVM 自身的内存需求（元空间和其他）减去一些内存，并在其组件（JVM 堆、堆外、任务管理器以及网络、托管内存等）之间自动划分和配置其余部分。

这些值被配置为内存大小，例如： *1536m* 或 *2g*。

**Parallelism**

- `taskmanager.numberOfTaskSlots`: TaskManager 提供的 slots *（默认值：1）*。每个 slot 可以执行一项任务或管道。
  在 TaskManager 中拥有多个 slots 可以帮助分摊并行任务或管道中的某些恒定开销（JVM、应用程序库或网络连接）。有关详细信息，请参阅 [Task Slots and Resources]({{< ref "docs/concepts/flink-architecture" >}}#task-slots-and-resources) 概念部分。

  使用一个 slot 运行更多更小的 TaskManager 是一个很好的起点，并导致任务之间的最佳隔离。将相同的资源分配给具有更多 slot 的更少、更大的 TaskManager 有助于提高资源利用率，但代价是任务之间的隔离更弱（更多任务共享同一个 JVM）。

- `parallelism.default`: 在任何地方都没有指定并行度时使用的默认并行度 *（默认值：1）*。

**Checkpointing**

您可以直接在 Flink 作业或应用程序中的代码中配置检查点。将这些值放在配置中将它们定义为默认值，以防应用程序未配置任何内容。

- `state.backend`: 要使用的状态后端。这定义了生成快照的数据结构机制。常见的值是 `filesystem` 或 `rocksdb`。
- `state.checkpoints.dir`: 要写入检查点的目录。这需要一个路径 URI如：*s3://mybucket/flink-app/checkpoints* 或 *hdfs://namenode:port/flink/checkpoints*。
- `state.savepoints.dir`: 保存点的默认目录。采用路径 URI，类似于 `state.checkpoints.dir`。
- `execution.checkpointing.interval`: 基本间隔设置。要启用检查点，您需要将此值设置为大于 0。

**Web UI**

- `web.submit.enable`: 通过 Flink UI 启用上传和启动作业（默认为 true）。请注意，即使禁用此功能，会话集群仍会通过 REST 请求（HTTP 调用）接受作业。此配置仅适用于在 UI 中上传作业的功能。
- `web.cancel.enable`: 通过 Flink UI 启用取消作业（默认为 true）。请注意，即使禁用此功能，会话集群仍会通过 REST 请求（HTTP 调用）取消作业。此配置仅适用于在 UI 中取消作业的功能。
- `web.upload.dir`: 存储上传作业的目录。仅在 `web.submit.enable` 为 true 时生效。
- `web.exception-history-size`: 设置打印 Flink 为作业处理的最近失败的异常历史记录的大小。

**其他**

- `io.tmp.dirs`: Flink 放置本地数据的目录，默认为系统临时目录（`java.io.tmpdir` 属性）。如果配置了目录列表，Flink 将在目录之间轮询写文件。

  这些目录下的数据默认包括 RocksDB 创建的文件、溢出的中间结果（批处理算法）和缓存的 jar 文件。

  持久性/恢复不依赖此数据，但如果此数据被删除，通常会导致重量级恢复操作。因此，建议将此设置该目录为不自动定期清除。

  默认情况下，Yarn 和 Kubernetes 设置会自动将此值配置为本地工作目录。

----
----

# 常用设置配置项

*配置 Flink 应用程序或集群的常用配置项。*

### 主机名和端口

为不同的 Flink 组件配置主机名和端口的配置项。

JobManager 主机名和端口在单机模式下生效，高可用模式下无效。
在该设置中，TaskManager 使用配置值来查找（并连接到）JobManager。
在所有高可用设置中，TaskManager 通过 High-Availability-Service（例如 ZooKeeper）发现 JobManager。

使用资源编排框架（K8s、Yarn）的设置通常使用框架的服务发现工具。

您不需要配置任何 TaskManager 主机和端口，除非设置需要使用特定的端口范围或特定的网络接口来绑定。

{{< generated/common_host_port_section >}}

### 容错

这些配置配置项控制 Flink 在执行过程中出现故障时的重启行为。
你在 `flink-conf.yaml` 中配置这些配置项决定了集群的默认重启策略。

默认重启策略只有在没有通过 `ExecutionConfig` 配置特定于作业的重启策略时才会生效。

{{< generated/restart_strategy_configuration >}}

**固定延迟重启策略**

{{< generated/fixed_delay_restart_strategy_configuration >}}

**故障率重启策略**

{{< generated/failure_rate_restart_strategy_configuration >}}

### 检查点和状态后端

这些配置项控制状态后端和检查点行为的基本设置。

这些配置项仅与以无界流方式执行的作业应用程序相关。
以批处理方式执行的作业应用程序不使用状态后端和检查点，而是使用针对批处理优化的不同内部数据结构。

{{< generated/common_state_backends_section >}}

### 高可用

这里的高可用性指的是 JobManager 进程从故障中恢复的能力。

JobManager 确保在跨 TaskManager 恢复期间的一致性。为了让 JobManager 本身始终如一地恢复，外部服务必须存储最少量的恢复元数据（例如“上次提交的 checkpoint 的 ID”），以及帮助选择和锁定哪个 JobManager 是领导者（以避免裂脑）。

{{< generated/common_high_availability_section >}}

**使用 ZooKeeper 进行高可用性设置的配置项**

{{< generated/common_high_availability_zk_section >}}

### 内存配置

这些配置值控制 TaskManager 和 JobManager 使用内存的方式。

Flink 试图尽可能地使用户免受配置 JVM 以进行数据密集型处理的复杂性。
大多数情况下，用户只需要设置 `taskmanager.memory.process.size` 或 `taskmanager.memory.flink.size` 的值（取决于如何设置），并可能调整 JVM 堆和 Managed Memory 的比例通过 `taskmanager.memory.managed.fraction`。下面的其他配置项可用于性能调整和修复与内存相关的错误。

有关这些配置项如何相互作用的详细说明，
请参阅有关 [TaskManager]({{< ref "docs/deployment/memory/mem_setup_tm" >}}) 和 [JobManager]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}) 内存配置的文档。

{{< generated/common_memory_section >}}

### 其他配置项

{{< generated/common_miscellaneous_section >}}

----
----

# 安全

用于配置 Flink 的安全性以及与外部系统的安全交互的配置项。

### SSL

Flink 的网络连接可以通过 SSL 进行保护。有关详细的设置指南和背景，请参阅 [SSL 设置文档]({{< ref "docs/deployment/security/security-ssl" >}})。

{{< generated/security_ssl_section >}}


### 使用外部系统进行身份验证

**ZooKeeper 认证授权**

连接到安全的 ZooKeeper quorum时，这些配置项项是必需的。

{{< generated/security_auth_zk_section >}}

**基于 Kerberos 的身份验证授权**

请参阅 [Flink 和 Kerberos 文档]({{< ref "docs/deployment/security/security-kerberos" >}}) 以获取设置指南和 Flink 可以通过 Kerberos 对其进行身份验证的外部系统列表。

{{< generated/security_auth_kerberos_section >}}

----
----

# 资源编排框架

本节包含与将 Flink 与资源编排框架（如 Kubernetes、Yarn 等）集成的相关配置。

请注意，并不总是需要将 Flink 与资源编排框架集成。
例如，您可以轻松地在 Kubernetes 上部署 Flink 应用程序，而 Flink 不知道它在 Kubernetes 上运行（并且无需在此处指定任何 Kubernetes 配置配置项。）请参阅 [this setup guide]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}) 为例。

本节中的配置项对于 Flink 本身主动从编排器请求和释放资源的设置是必需的。

### YARN

{{< generated/yarn_config_configuration >}}

### Kubernetes

{{< generated/kubernetes_config_configuration >}}


----
----

# 状态后端

有关状态后端的背景信息，请参阅 [状态后端文档]({{< ref "docs/ops/state/state_backends" >}})。

### RocksDB State Backend

这些是配置 RocksDB 状态后端所需的基本配置项。有关进阶底层配置和故障排除所需的配置项，请参阅 [Advanced RocksDB Backend Section](#advanced-rocksdb-state-backends-options)。

{{< generated/state_backend_rocksdb_section >}}

----
----

# 指标

有关 Flink 指标基础架构的背景信息，请参阅 [指标系统文档]({{< ref "docs/ops/metrics" >}})。

{{< generated/metric_configuration >}}

### RocksDB 原生指标

对于使用 RocksDB 状态后端的应用程序，Flink 可以从 RocksDB 的源码中报告指标。
此处的指标仅限于运算符，然后按列族进一步细分；值为无符号长整型。

{{< hint warning >}}
启用 RocksDB 的原生指标可能会导致性能下降，应谨慎设置。
{{< /hint >}}

{{< generated/rocksdb_native_metric_configuration >}}

----
----

# History Server

历史服务器保存已完成作业的信息（图表、运行时间、统计信息）。要启用它，您必须在 JobManager (`jobmanager.archive.fs.dir`) 中启用 "job archiving"。

有关详细信息，请参阅 [历史服务器文档]({{< ref "docs/deployment/advanced/historyserver" >}})。

{{< generated/history_server_configuration >}}

----
----

# 实验性的功能

*Flink 中实验性功能的配置项。*

### 可查询状态

*可查询状态*是一项实验性功能，可让您像键值存储一样访问 Flink 的内部状态。
有关详细信息，请参阅 [可查询状态文档]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}})。

{{< generated/queryable_state_configuration >}}

----
----

# 客户端

{{< generated/client_configuration >}}

----
----

# Execution

{{< generated/deployment_configuration >}}
{{< generated/savepoint_config_configuration >}}
{{< generated/execution_configuration >}}

### 管道

{{< generated/pipeline_configuration >}}
{{< generated/stream_pipeline_configuration >}}

### 检查点

{{< generated/execution_checkpointing_configuration >}}

----
----

# 调试和专家调优

{{< hint warning >}}
下面的配置项适用于专家用户和修复调试问题。大多数设置不需要配置这些配置项。
{{< /hint >}}

### 类加载

Flink 动态加载提交到会话集群的作业的代码。此外，Flink 试图对应用程序隐藏类路径中的许多依赖项。这有助于减少应用程序代码和类路径中的依赖项之间的依赖项冲突。

详情请参阅[调试类加载文档]({{< ref "docs/ops/debugging/debugging_classloading" >}})。

{{< generated/expert_class_loading_section >}}

### 调试的高级配置项

{{< generated/expert_debugging_and_tuning_section >}}

### 状态后端高级配置项

{{< generated/expert_state_backends_section >}}

### 追踪状态后端延迟配置项

{{< generated/state_backend_latency_tracking_section >}}

### RocksDB 状态后端高级配置项

调优 RocksDB 和 RocksDB 检查点的进阶配置项。

{{< generated/expert_rocksdb_section >}}

**RocksDB 可配置项**

这些配置项可以对 ColumnFamilies 的行为和资源进行细粒度控制。
随着`state.backend.rocksdb.memory.managed`和`state.backend.rocksdb.memory.fixed-per-slot`（Apache Flink 1.10）的引入，应该只需要使用这里的配置项来获得高级性能调优。这里的这些配置项也可以通过`RocksDBStateBackend.setRocksDBOptions(RocksDBOptionsFactory)`在应用程序中指定。

{{< generated/rocksdb_configurable_configuration >}}

### 容错高级配置项

*这些参数可以帮助解决与故障转移和组件错误地将彼此视为故障相关的问题。*

{{< generated/expert_fault_tolerance_section >}}

### 集群高级配置项

{{< generated/expert_cluster_section >}}

### 进阶 JobManager 配置

{{< generated/expert_jobmanager_section >}}

### 进阶调度配置

*这些参数可以帮助微调特定情况的调度。*

{{< generated/expert_scheduling_section >}}

### 高可用性高级配置项

{{< generated/expert_high_availability_section >}}

### 高可用性 ZooKeeper 高级配置项

{{< generated/expert_high_availability_zk_section >}}

### 高可用性 Kubernetes 高级配置项

{{< generated/expert_high_availability_k8s_section >}}

### SSL 安全高级配置项

{{< generated/expert_security_ssl_section >}}

### REST 端点和客户端的高级配置项

{{< generated/expert_rest_section >}}

### Flink Web UI 的高级配置项

{{< generated/web_configuration >}}

### 所有的 JobManager 配置项

**JobManager**

{{< generated/all_jobmanager_section >}}

**Blob Server**

Blob Server 是 JobManager 中的一个组件。它用于分发太大而无法附加到 RPC 消息并受益于缓存的对象（如 Jar 文件或大型序列化代码对象）。

{{< generated/blob_server_configuration >}}

**资源管理器**

这些配置项控制基本的资源管理器行为，独立于使用的资源编排管理框架（YARN 等）。

{{< generated/resource_manager_configuration >}}

### Full TaskManagerOptions

有关如何使用`taskmanager.network.memory.buffer-debloat.*` 配置的详细信息，请参阅[网络内存调整指南]({{< ref "docs/deployment/memory/network_mem_tuning" >}})。

{{< generated/all_taskmanager_section >}}

**数据传输网络堆栈**

这些配置项用于 TaskManager 之间的流和批处理数据交换的网络堆栈。

{{< generated/all_taskmanager_network_section >}}

### RPC / Akka

Flink 使用 Akka 进行组件之间的 RPC（JobManager/TaskManager/ResourceManager）。
Flink 不使用 Akka 进行数据传输。

{{< generated/akka_configuration >}}

----
----

# JVM 和日志配置

{{< generated/environment_configuration >}}

# 转发环境变量

您可以在 Yarn 上启动的 JobManager 和 TaskManager 进程上配置要设置的环境变量。

- `containerized.master.env.`: 将自定义环境变量传递给 Flink 的 JobManager 进程的前缀。
例如，要将 LD_LIBRARY_PATH 作为环境变量传递给 JobManager，请在 flink-conf.yaml 中设置 containerized.master.env.LD_LIBRARY_PATH: "/usr/lib/native"。

- `containerized.taskmanager.env.`: 与上述类似，此配置前缀可以在工作节点（TaskManagers）设置自定义环境变量。

----
----

# 弃用的配置项

这些配置项与 Flink 中不再积极开发的部分有关。
这些配置项可能会在未来的版本中删除。

**DataSet API 优化器**

{{< generated/optimizer_configuration >}}

**DataSet API 运行时算法**

{{< generated/algorithm_configuration >}}

**DataSet File Sinks**

{{< generated/deprecated_file_sinks_section >}}

{{< top >}}
