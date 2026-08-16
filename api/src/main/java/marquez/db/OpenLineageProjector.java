/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDefaults.DEFAULT_NAMESPACE_OWNER;
import static marquez.db.OpenLineageDefaults.DEFAULT_SOURCE_NAME;
import static marquez.db.OpenLineageDefaults.EMPTY_RUN_ARGS_CHECKSUM;
import static marquez.db.OpenLineageDefaults.EMPTY_RUN_ARGS_JSON;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetType;
import marquez.common.models.JobType;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunState;
import marquez.common.models.SourceType;
import marquez.db.ColumnLineageDao.ColumnLineageDatasetWrite;
import marquez.db.ColumnLineageDao.ColumnLineageWrite;
import marquez.db.DatasetDao.DatasetCurrentVersionUpdate;
import marquez.db.DatasetDao.DatasetUpsert;
import marquez.db.DatasetFacetsDao.DatasetFacetWrite;
import marquez.db.DatasetFieldDao.DatasetFieldMapping;
import marquez.db.DatasetFieldDao.DatasetFieldUpsert;
import marquez.db.DatasetSymlinkDao.DatasetSymlinkKey;
import marquez.db.DatasetSymlinkDao.PlannedDatasetSymlinkUpsert;
import marquez.db.JobVersionDao.BagOfJobVersionInfo;
import marquez.db.JobVersionDao.IoType;
import marquez.db.JobVersionDao.JobDataset;
import marquez.db.JobVersionDao.JobRowRunDetails;
import marquez.db.RunDao.RunUpsert;
import marquez.db.models.ColumnLineageRow;
import marquez.db.models.DatasetFieldRow;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetSymlinkRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.InputFieldData;
import marquez.db.models.JobRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.ModelDaos;
import marquez.db.models.NamespaceRow;
import marquez.db.models.ProjectionOrder;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunIoSnapshot;
import marquez.db.models.RunIoState;
import marquez.db.models.RunRow;
import marquez.db.models.RunStateRow;
import marquez.db.models.SourceRow;
import marquez.db.models.UpdateLineageRow;
import marquez.db.models.UpdateLineageRow.DatasetRecord;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import marquez.service.models.LineageEvent.DatasetFacets;
import marquez.service.models.LineageEvent.DocumentationJobFacet;
import marquez.service.models.LineageEvent.Job;
import marquez.service.models.LineageEvent.JobFacet;
import marquez.service.models.LineageEvent.LifecycleStateChangeFacet;
import marquez.service.models.LineageEvent.NominalTimeRunFacet;
import marquez.service.models.LineageEvent.ParentRunFacet;
import marquez.service.models.LineageEvent.RunFacet;
import marquez.service.models.LineageEvent.SchemaDatasetFacet;
import marquez.service.models.LineageEvent.SchemaField;
import marquez.service.models.LineageEvent.SymlinkIdentifier;
import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OpenLineageProjector {
  private static final Logger LOG = LoggerFactory.getLogger(OpenLineageProjector.class);
  private static final String OPEN_LINEAGE_SOURCE_TYPE = SourceType.of("POSTGRESQL").getValue();
  private static final DatasetType OPEN_LINEAGE_DATASET_TYPE = DatasetType.DB_TABLE;
  public static final OpenLineageProjector INSTANCE = new OpenLineageProjector();

  private OpenLineageProjector() {}

  public static OpenLineageProjector getInstance() {
    return INSTANCE;
  }

  /** Exact serialized input plus the listener decision made before entering the transaction. */
  public record ProjectionRequest(
      BaseEvent event, String exactEventJson, boolean listenerSnapshotRequired) {
    public ProjectionRequest {
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(exactEventJson, "exactEventJson");
      if (exactEventJson.isBlank()) {
        throw new IllegalArgumentException("exactEventJson must not be blank");
      }
      eventInstant(event);
    }

    public ProjectionOrder order() {
      return new ProjectionOrder(eventInstant(event), Utils.sha256Utf8(exactEventJson));
    }
  }

  public sealed interface ProjectionResult
      permits RunProjectionResult, JobProjectionResult, DatasetProjectionResult {
    ProjectionRequest request();
  }

  /** Immutable material required for run listeners and post-commit publication. */
  public record RunProjectionResult(
      ProjectionRequest request,
      NamespaceRow namespace,
      JobRow job,
      RunArgsRow runArgs,
      RunRow run,
      @Nullable RunStateRow runState,
      Optional<List<DatasetProjection>> inputs,
      Optional<List<DatasetProjection>> outputs,
      @Nullable RunIoSnapshot runIoSnapshot,
      @Nullable JobVersionProjection jobVersion)
      implements ProjectionResult {
    public RunProjectionResult {
      Objects.requireNonNull(request, "request");
      Objects.requireNonNull(namespace, "namespace");
      Objects.requireNonNull(job, "job");
      Objects.requireNonNull(runArgs, "runArgs");
      Objects.requireNonNull(run, "run");
      inputs = immutableIoProjection(inputs, "inputs");
      outputs = immutableIoProjection(outputs, "outputs");
    }
  }

  /** Immutable result of projecting a runless job event. */
  public record JobProjectionResult(
      ProjectionRequest request,
      NamespaceRow namespace,
      JobRow job,
      Optional<List<DatasetProjection>> inputs,
      Optional<List<DatasetProjection>> outputs,
      JobVersionProjection jobVersion)
      implements ProjectionResult {
    public JobProjectionResult {
      Objects.requireNonNull(request, "request");
      Objects.requireNonNull(namespace, "namespace");
      Objects.requireNonNull(job, "job");
      inputs = immutableIoProjection(inputs, "inputs");
      outputs = immutableIoProjection(outputs, "outputs");
      Objects.requireNonNull(jobVersion, "jobVersion");
    }
  }

  /** Immutable subset of a projected job version used by listeners and result consumers. */
  public record JobVersionProjection(
      JobRow job,
      JobVersionRow jobVersion,
      List<ExtendedDatasetVersionRow> inputs,
      List<ExtendedDatasetVersionRow> outputs) {
    public JobVersionProjection {
      Objects.requireNonNull(job, "job");
      Objects.requireNonNull(jobVersion, "jobVersion");
      inputs = List.copyOf(Objects.requireNonNull(inputs, "inputs"));
      outputs = List.copyOf(Objects.requireNonNull(outputs, "outputs"));
    }
  }

  /** Immutable result of projecting a dataset event. */
  public record DatasetProjectionResult(
      ProjectionRequest request, NamespaceRow namespace, List<DatasetProjection> outputs)
      implements ProjectionResult {
    public DatasetProjectionResult {
      Objects.requireNonNull(request, "request");
      Objects.requireNonNull(namespace, "namespace");
      outputs = List.copyOf(Objects.requireNonNull(outputs, "outputs"));
    }
  }

  /** Immutable dataset occurrence projected from one side of an event payload. */
  public record DatasetProjection(
      DatasetRow dataset,
      DatasetVersionRow version,
      NamespaceRow namespace,
      List<ColumnLineageRow> columnLineage) {
    public DatasetProjection {
      Objects.requireNonNull(dataset, "dataset");
      Objects.requireNonNull(version, "version");
      Objects.requireNonNull(namespace, "namespace");
      columnLineage = List.copyOf(Objects.requireNonNull(columnLineage, "columnLineage"));
    }
  }

  /**
   * Projects an ordered batch using DAOs attached to the caller's transaction. This method never
   * opens or nests a transaction, and results retain input order one-for-one.
   */
  public List<ProjectionResult> projectBatchInTransaction(
      BaseDao transactionalDao, ObjectMapper mapper, List<ProjectionRequest> requests) {
    Objects.requireNonNull(transactionalDao, "transactionalDao");
    Objects.requireNonNull(mapper, "mapper");
    List<ProjectionRequest> immutableRequests =
        List.copyOf(Objects.requireNonNull(requests, "requests"));
    ProjectionTransactionContext transaction =
        new ProjectionTransactionContext(transactionalDao, mapper);
    transaction.datasetAliasPlan = planDatasetAliases(transaction, immutableRequests);
    List<ProjectionResult> results = new ArrayList<>(immutableRequests.size());
    for (ProjectionRequest request : immutableRequests) {
      results.add(projectInTransaction(transaction, request, request.order()));
    }
    return List.copyOf(results);
  }

  public ProjectionResult projectInTransaction(
      BaseDao transactionalDao, ObjectMapper mapper, ProjectionRequest request) {
    return projectBatchInTransaction(transactionalDao, mapper, List.of(request)).get(0);
  }

  private ProjectionResult projectInTransaction(
      ProjectionTransactionContext transaction, ProjectionRequest request, ProjectionOrder order) {
    BaseEvent event = request.event();
    if (event instanceof LineageEvent lineageEvent) {
      return toRunProjectionResult(
          request,
          updateMarquezModelInTransaction(
              transaction, lineageEvent, request.listenerSnapshotRequired(), order));
    }
    if (event instanceof JobEvent jobEvent) {
      return toJobProjectionResult(
          request, updateMarquezModelInTransaction(transaction, jobEvent, order));
    }
    if (event instanceof DatasetEvent datasetEvent) {
      return toDatasetProjectionResult(
          request, updateMarquezModelInTransaction(transaction, datasetEvent, order));
    }
    throw new IllegalArgumentException(
        "Unsupported OpenLineage event type: " + event.getClass().getName());
  }

  private static RunProjectionResult toRunProjectionResult(
      ProjectionRequest request, UpdateLineageRow row) {
    return new RunProjectionResult(
        request,
        row.getNamespace(),
        row.getJob(),
        row.getRunArgs(),
        row.getRun(),
        row.getRunState(),
        toDatasetProjections(row.getInputs()),
        toDatasetProjections(row.getOutputs()),
        row.getRunIoSnapshot(),
        toJobVersionProjection(row.getJobVersionBag()));
  }

  private static JobProjectionResult toJobProjectionResult(
      ProjectionRequest request, UpdateLineageRow row) {
    return new JobProjectionResult(
        request,
        row.getNamespace(),
        row.getJob(),
        toDatasetProjections(row.getInputs()),
        toDatasetProjections(row.getOutputs()),
        Objects.requireNonNull(
            toJobVersionProjection(row.getJobVersionBag()),
            "Job projection did not produce a job version"));
  }

  private static @Nullable JobVersionProjection toJobVersionProjection(
      @Nullable BagOfJobVersionInfo bag) {
    return bag == null
        ? null
        : new JobVersionProjection(
            bag.getJobRow(), bag.getJobVersionRow(), bag.getInputs(), bag.getOutputs());
  }

  private static DatasetProjectionResult toDatasetProjectionResult(
      ProjectionRequest request, UpdateLineageRow row) {
    return new DatasetProjectionResult(
        request,
        row.getNamespace(),
        toDatasetProjections(
            row.getOutputs()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Dataset projection did not produce an output"))));
  }

  private static Optional<List<DatasetProjection>> immutableIoProjection(
      Optional<List<DatasetProjection>> io, String name) {
    return Objects.requireNonNull(io, name).map(List::copyOf);
  }

  private static Optional<List<DatasetProjection>> toDatasetProjections(
      Optional<List<DatasetRecord>> records) {
    return Objects.requireNonNull(records, "records")
        .map(OpenLineageProjector::toDatasetProjections);
  }

  private static List<DatasetProjection> toDatasetProjections(List<DatasetRecord> records) {
    return records.stream()
        .map(
            record ->
                new DatasetProjection(
                    record.getDatasetRow(),
                    record.getDatasetVersionRow(),
                    record.getNamespaceRow(),
                    record.getColumnLineageRows()))
        .toList();
  }

  private static Instant eventInstant(BaseEvent event) {
    if (event instanceof LineageEvent lineageEvent) {
      return lineageEvent.getEventTime().toInstant();
    }
    if (event instanceof JobEvent jobEvent) {
      return jobEvent.getEventTime().toInstant();
    }
    if (event instanceof DatasetEvent datasetEvent) {
      return datasetEvent.getEventTime().toInstant();
    }
    throw new IllegalArgumentException(
        "Unsupported OpenLineage event type: " + event.getClass().getName());
  }

  /** Plans the complete dataset alias graph before the first ordered event is projected. */
  private DatasetAliasPlan planDatasetAliases(
      ProjectionTransactionContext transaction, List<ProjectionRequest> requests) {
    DatasetAliasGraph graph = new DatasetAliasGraph();
    for (ProjectionRequest request : requests) {
      BaseEvent event = request.event();
      Instant eventTime = eventInstant(event);
      if (event instanceof LineageEvent lineageEvent) {
        graph.addAll(lineageEvent.getInputs(), eventTime);
        graph.addAll(lineageEvent.getOutputs(), eventTime);
      } else if (event instanceof JobEvent jobEvent) {
        graph.addAll(jobEvent.getInputs(), eventTime);
        graph.addAll(jobEvent.getOutputs(), eventTime);
      } else if (event instanceof DatasetEvent datasetEvent) {
        graph.add(datasetEvent.getDataset(), eventTime);
      }
    }
    if (graph.parent.isEmpty()) {
      return new DatasetAliasPlan(Map.of());
    }

    List<DatasetIdentity> storageIdentities =
        graph.metadataByStorage.keySet().stream().sorted().toList();
    Map<DatasetIdentity, NamespaceRow> namespacesByIdentity = new LinkedHashMap<>();
    List<DatasetSymlinkKey> lookupKeys = new ArrayList<>(storageIdentities.size());
    Map<DatasetSymlinkKey, DatasetIdentity> identityByStorageKey = new LinkedHashMap<>();
    for (DatasetIdentity identity : storageIdentities) {
      transaction
          .findNamespace(identity.namespace())
          .ifPresent(
              namespace -> {
                namespacesByIdentity.put(identity, namespace);
                DatasetSymlinkKey storageKey =
                    new DatasetSymlinkKey(namespace.getUuid(), identity.name());
                identityByStorageKey.put(storageKey, identity);
                lookupKeys.add(storageKey);
              });
    }

    List<DatasetSymlinkRow> existingRows =
        transaction.daos.getDatasetSymlinkDao().findDatasetSymlinksByKeysInTransaction(lookupKeys);
    Set<DatasetIdentity> persistedPrimaryAliasCollisions = new LinkedHashSet<>();
    for (DatasetSymlinkRow existing : existingRows) {
      DatasetIdentity physicalIdentity =
          identityByStorageKey.get(
              new DatasetSymlinkKey(existing.getNamespaceUuid(), existing.getName()));
      if (physicalIdentity != null
          && existing.isPrimary()
          && !graph.metadata(physicalIdentity).primary()) {
        persistedPrimaryAliasCollisions.add(physicalIdentity);
      }
    }
    graph.finalizeComponents(persistedPrimaryAliasCollisions);

    Map<DatasetIdentity, Set<UUID>> existingDatasetUuidsByComponent = new LinkedHashMap<>();
    for (DatasetSymlinkRow existing : existingRows) {
      DatasetIdentity physicalIdentity =
          identityByStorageKey.get(
              new DatasetSymlinkKey(existing.getNamespaceUuid(), existing.getName()));
      if (physicalIdentity != null && !graph.isProtectedAlias(physicalIdentity)) {
        DatasetIdentity component = graph.componentForStorage(physicalIdentity);
        existingDatasetUuidsByComponent
            .computeIfAbsent(component, ignored -> new LinkedHashSet<>())
            .add(existing.getUuid());
      }
    }

    Map<DatasetIdentity, UUID> canonicalUuidByComponent = new LinkedHashMap<>();
    for (DatasetIdentity component : graph.components()) {
      Set<UUID> existingUuids = existingDatasetUuidsByComponent.getOrDefault(component, Set.of());
      if (existingUuids.size() > 1) {
        throw new IllegalArgumentException(
            "Dataset alias component resolves to multiple canonical datasets: "
                + graph.identitiesInComponent(component)
                + " -> "
                + existingUuids);
      }
      UUID canonicalUuid =
          existingUuids.stream()
              .findFirst()
              .orElseGet(
                  () -> {
                    DatasetIdentity smallest = graph.smallestNormalizedIdentity(component);
                    return Utils.toNameBasedUuid(smallest.namespace(), smallest.name());
                  });
      canonicalUuidByComponent.put(component, canonicalUuid);
    }

    for (DatasetIdentity identity : storageIdentities) {
      namespacesByIdentity.put(
          identity,
          transaction.upsertNamespace(identity.namespace(), graph.metadata(identity).now()));
    }

    List<PlannedDatasetSymlinkUpsert> writes = new ArrayList<>(storageIdentities.size());
    for (DatasetIdentity identity : storageIdentities) {
      if (graph.isProtectedAlias(identity)) {
        continue;
      }
      DatasetAliasMetadata metadata = graph.metadata(identity);
      UUID canonicalUuid = canonicalUuidByComponent.get(graph.componentForStorage(identity));
      writes.add(
          new PlannedDatasetSymlinkUpsert(
              canonicalUuid,
              identity.name(),
              namespacesByIdentity.get(identity).getUuid(),
              metadata.primary(),
              metadata.primary() ? null : metadata.type(),
              metadata.now()));
    }
    List<DatasetSymlinkRow> resolved =
        transaction.daos.getDatasetSymlinkDao().resolvePlannedSymlinksInTransaction(writes);

    Map<DatasetIdentity, DatasetSymlinkRow> rowByStorageIdentity = new LinkedHashMap<>();
    int resolvedIndex = 0;
    for (DatasetIdentity physicalIdentity : storageIdentities) {
      if (graph.isProtectedAlias(physicalIdentity)) {
        continue;
      }
      DatasetSymlinkRow row = resolved.get(resolvedIndex++);
      UUID expectedUuid = canonicalUuidByComponent.get(graph.componentForStorage(physicalIdentity));
      if (!expectedUuid.equals(row.getUuid())) {
        throw new IllegalArgumentException(
            "Dataset alias identity was concurrently mapped to a different canonical dataset: "
                + physicalIdentity
                + " -> "
                + row.getUuid()
                + " (expected "
                + expectedUuid
                + ")");
      }
      rowByStorageIdentity.put(physicalIdentity, row);
    }

    Map<DatasetIdentity, DatasetSymlinkRow> canonicalRows = new LinkedHashMap<>();
    for (DatasetIdentity identity : graph.payloadPrimaryIdentities()) {
      DatasetSymlinkRow row = rowByStorageIdentity.get(identity);
      if (row == null) {
        throw new IllegalStateException(
            "Dataset alias plan omitted normalized identity " + identity);
      }
      canonicalRows.put(identity, row);
    }
    return new DatasetAliasPlan(canonicalRows);
  }

  private record DatasetIdentity(String namespace, String name)
      implements Comparable<DatasetIdentity> {
    private DatasetIdentity {
      Objects.requireNonNull(namespace, "namespace");
      Objects.requireNonNull(name, "name");
    }

    @Override
    public int compareTo(DatasetIdentity other) {
      int namespaceComparison = namespace.compareTo(other.namespace);
      return namespaceComparison != 0 ? namespaceComparison : name.compareTo(other.name);
    }
  }

  private record DatasetAliasMetadata(boolean primary, @Nullable String type, Instant now) {
    private DatasetAliasMetadata merge(
        boolean nextPrimary, @Nullable String nextType, Instant nextNow) {
      boolean mergedPrimary = primary || nextPrimary;
      String mergedType = mergedPrimary ? null : lexicographicallyFirst(type, nextType);
      return new DatasetAliasMetadata(
          mergedPrimary, mergedType, now.compareTo(nextNow) <= 0 ? now : nextNow);
    }

    private static @Nullable String lexicographicallyFirst(
        @Nullable String left, @Nullable String right) {
      if (left == null || right == null) {
        return left == null ? right : left;
      }
      return left.compareTo(right) <= 0 ? left : right;
    }
  }

  private record DatasetAliasPlan(Map<DatasetIdentity, DatasetSymlinkRow> rowsByIdentity) {
    private DatasetAliasPlan {
      rowsByIdentity = Map.copyOf(rowsByIdentity);
    }
  }

  /** Minimal union-find plus persisted-identity metadata for one batch. */
  private final class DatasetAliasGraph {
    private final Map<DatasetIdentity, DatasetIdentity> parent = new LinkedHashMap<>();
    private final Map<DatasetIdentity, DatasetAliasMetadata> metadataByStorage =
        new LinkedHashMap<>();
    private final Set<DatasetIdentity> payloadPrimaryIdentities = new LinkedHashSet<>();
    private final List<DatasetAliasEdge> aliasEdges = new ArrayList<>();
    private final Set<DatasetIdentity> protectedStorageAliases = new LinkedHashSet<>();

    private void addAll(@Nullable List<Dataset> datasets, Instant eventTime) {
      if (datasets != null) {
        for (Dataset dataset : datasets) {
          add(dataset, eventTime);
        }
      }
    }

    private void add(Dataset dataset, Instant eventTime) {
      DatasetIdentity primary = normalizedIdentity(dataset.getNamespace(), dataset.getName());
      parent.putIfAbsent(primary, primary);
      payloadPrimaryIdentities.add(primary);
      addStorageIdentity(primary, true, null, eventTime);

      for (SymlinkIdentifier alias : symlinkIdentifiers(dataset)) {
        DatasetIdentity normalizedAlias = normalizedIdentity(alias.getNamespace(), alias.getName());
        parent.putIfAbsent(normalizedAlias, normalizedAlias);
        aliasEdges.add(new DatasetAliasEdge(primary, normalizedAlias));
        addStorageIdentity(normalizedAlias, false, alias.getType(), eventTime);
      }
    }

    private DatasetIdentity normalizedIdentity(String namespace, String name) {
      return new DatasetIdentity(Utils.sanitizeOpenLineageNamespace(namespace), name);
    }

    private void addStorageIdentity(
        DatasetIdentity storageIdentity, boolean primary, @Nullable String type, Instant now) {
      metadataByStorage.compute(
          storageIdentity,
          (ignored, previous) ->
              previous == null
                  ? new DatasetAliasMetadata(primary, primary ? null : type, now)
                  : previous.merge(primary, type, now));
    }

    private DatasetIdentity find(DatasetIdentity identity) {
      DatasetIdentity directParent = parent.get(identity);
      if (directParent == null) {
        throw new IllegalStateException("Unknown normalized dataset identity " + identity);
      }
      if (!directParent.equals(identity)) {
        directParent = find(directParent);
        parent.put(identity, directParent);
      }
      return directParent;
    }

    private void union(DatasetIdentity left, DatasetIdentity right) {
      DatasetIdentity leftRoot = find(left);
      DatasetIdentity rightRoot = find(right);
      if (leftRoot.equals(rightRoot)) {
        return;
      }
      DatasetIdentity smaller = leftRoot.compareTo(rightRoot) <= 0 ? leftRoot : rightRoot;
      DatasetIdentity larger = smaller.equals(leftRoot) ? rightRoot : leftRoot;
      parent.put(larger, smaller);
    }

    private void finalizeComponents(Set<DatasetIdentity> persistedPrimaryAliasCollisions) {
      protectedStorageAliases.addAll(persistedPrimaryAliasCollisions);
      Set<DatasetIdentity> protectedEdgeAliases =
          new LinkedHashSet<>(persistedPrimaryAliasCollisions);
      for (DatasetAliasEdge edge : aliasEdges) {
        if (!edge.primary().equals(edge.alias())
            && payloadPrimaryIdentities.contains(edge.alias())) {
          protectedEdgeAliases.add(edge.alias());
        }
      }
      parent.replaceAll((identity, ignored) -> identity);
      for (DatasetAliasEdge edge : aliasEdges) {
        if (!protectedEdgeAliases.contains(edge.alias())) {
          union(edge.primary(), edge.alias());
        }
      }
    }

    private DatasetAliasMetadata metadata(DatasetIdentity storageIdentity) {
      return Objects.requireNonNull(metadataByStorage.get(storageIdentity));
    }

    private DatasetIdentity componentForStorage(DatasetIdentity storageIdentity) {
      return find(storageIdentity);
    }

    private boolean isProtectedAlias(DatasetIdentity identity) {
      return protectedStorageAliases.contains(identity);
    }

    private List<DatasetIdentity> components() {
      return parent.keySet().stream().map(this::find).distinct().sorted().toList();
    }

    private List<DatasetIdentity> payloadPrimaryIdentities() {
      return payloadPrimaryIdentities.stream().sorted().toList();
    }

    private List<DatasetIdentity> identitiesInComponent(DatasetIdentity component) {
      return parent.keySet().stream()
          .filter(identity -> find(identity).equals(component))
          .sorted()
          .toList();
    }

    private DatasetIdentity smallestNormalizedIdentity(DatasetIdentity component) {
      return identitiesInComponent(component).get(0);
    }
  }

  private record DatasetAliasEdge(DatasetIdentity primary, DatasetIdentity alias) {}

  /** Owns all lazily attached model DAOs for exactly one caller-owned transaction. */
  private static final class ProjectionTransactionContext {
    private final ModelDaos daos;
    private final ObjectMapper mapper;
    private final Map<String, NamespaceRow> namespacesByExactName = new LinkedHashMap<>();
    private final Map<UUID, DatasetVersionRow> datasetVersionsByUuid = new LinkedHashMap<>();
    @Nullable private DatasetAliasPlan datasetAliasPlan;

    private ProjectionTransactionContext(BaseDao transactionalDao, ObjectMapper mapper) {
      this.daos = new ModelDaos(transactionalDao);
      this.mapper = mapper;
    }

    private NamespaceRow upsertNamespace(String exactName, Instant now) {
      NamespaceRow namespace = namespacesByExactName.get(exactName);
      if (namespace == null || namespace.getIsHidden()) {
        namespace =
            daos.getNamespaceDao()
                .upsertNamespaceRow(UUID.randomUUID(), now, exactName, DEFAULT_NAMESPACE_OWNER);
        namespacesByExactName.put(exactName, namespace);
      }
      return namespace;
    }

    private Optional<NamespaceRow> findNamespace(String exactName) {
      NamespaceRow cached = namespacesByExactName.get(exactName);
      if (cached != null) {
        return Optional.of(cached);
      }
      Optional<NamespaceRow> found = daos.getNamespaceDao().findNamespaceByName(exactName);
      found.ifPresent(row -> namespacesByExactName.put(exactName, row));
      return found;
    }

    private Optional<DatasetVersionRow> findDatasetVersion(UUID versionUuid) {
      DatasetVersionRow cached = datasetVersionsByUuid.get(versionUuid);
      if (cached != null) {
        return Optional.of(cached);
      }
      Optional<DatasetVersionRow> found = daos.getDatasetVersionDao().findRowByUuid(versionUuid);
      found.ifPresent(row -> datasetVersionsByUuid.put(versionUuid, row));
      return found;
    }

    private void cacheDatasetVersion(DatasetVersionRow version) {
      datasetVersionsByUuid.put(version.getUuid(), version);
    }
  }

  private UpdateLineageRow updateMarquezModelInTransaction(
      ProjectionTransactionContext transaction,
      LineageEvent event,
      boolean listenerSnapshotRequired,
      ProjectionOrder order) {
    Instant now = event.getEventTime().toInstant();
    ModelDaos daos = transaction.daos;
    LineageWriteContext context = new LineageWriteContext(transaction, order);
    LineageProjectionResult projection =
        updateBaseMarquezModel(event, transaction.mapper, now, context);
    UpdateLineageRow updateLineageRow = projection.row();
    UUID effectiveRunUuid = updateLineageRow.getRun().getUuid();
    persistReportedRunIoState(daos, updateLineageRow, order);
    String eventType = event.getEventType();
    RunState runState = getRunState(eventType);
    boolean streaming = event.getJob() != null && event.getJob().isStreamingJob();
    boolean terminal = eventType != null && runState.isDone();

    RunIoSnapshot runIoSnapshot = null;
    if (streaming || terminal || (eventType != null && listenerSnapshotRequired)) {
      runIoSnapshot = daos.getJobVersionDao().findRunIoSnapshot(effectiveRunUuid);
      updateLineageRow.setRunIoSnapshot(runIoSnapshot);
    }

    if (streaming || terminal) {
      updateRunJobVersionInTransaction(
          event, now, projection, runState, daos, runIoSnapshot, order);
    }

    if (runIoSnapshot != null && (streaming || "complete".equalsIgnoreCase(eventType))) {
      updateOutputDatasetVersions(
          daos, runIoSnapshot, updateLineageRow.getOutputs().orElse(List.of()), now, order);
    }
    return updateLineageRow;
  }

  /**
   * Publishes each reported run-I/O side independently. A missing side has no write, an explicit
   * empty side writes an empty ordered snapshot, and a non-empty side replaces with payload order.
   */
  private void persistReportedRunIoState(
      ModelDaos daos, UpdateLineageRow row, ProjectionOrder order) {
    UUID runUuid = row.getRun().getUuid();
    List<RunIoState> writes =
        Stream.concat(
                row
                    .getInputs()
                    .map(records -> runIoState(runUuid, RunIoState.IoType.INPUT, order, records))
                    .stream(),
                row
                    .getOutputs()
                    .map(records -> runIoState(runUuid, RunIoState.IoType.OUTPUT, order, records))
                    .stream())
            .toList();
    if (!writes.isEmpty()) {
      daos.getRunIoStateDao().upsertAllInTransaction(writes);
    }
  }

  private static List<UUID> datasetVersionUuids(List<DatasetRecord> records) {
    return records.stream().map(record -> record.getDatasetVersionRow().getUuid()).toList();
  }

  private static RunIoState runIoState(
      UUID runUuid, RunIoState.IoType ioType, ProjectionOrder order, List<DatasetRecord> records) {
    return new RunIoState(runUuid, ioType, order, datasetVersionUuids(records));
  }

  private UpdateLineageRow updateMarquezModelInTransaction(
      ProjectionTransactionContext transaction, DatasetEvent event, ProjectionOrder order) {
    ModelDaos daos = transaction.daos;
    LineageWriteContext context = new LineageWriteContext(transaction, order);
    Instant now = event.getEventTime().toInstant();

    UpdateLineageRow bag = new UpdateLineageRow();
    NamespaceRow namespace =
        context.upsertNamespace(
            Utils.sanitizeOpenLineageNamespace(event.getDataset().getNamespace()), now);
    bag.setNamespace(namespace);

    Dataset dataset = event.getDataset();
    DatasetRecord record =
        upsertAndQueueLineageDatasets(context, List.of(dataset), now, null, null, false).get(0);
    context.flushDatasetFacets();
    context.flushColumnLineage(now);

    daos.getDatasetDao()
        .updateVersionsInTransaction(
            List.of(
                new DatasetCurrentVersionUpdate(
                    record.getDatasetRow().getUuid(),
                    now,
                    record.getDatasetVersionRow().getUuid())),
            order);

    bag.setOutputs(Optional.of(List.of(record)));
    return bag;
  }

  private UpdateLineageRow updateMarquezModelInTransaction(
      ProjectionTransactionContext transaction, JobEvent event, ProjectionOrder order) {
    ModelDaos daos = transaction.daos;
    LineageWriteContext context = new LineageWriteContext(transaction, order);
    Instant now = event.getEventTime().toInstant();

    UpdateLineageRow bag = new UpdateLineageRow();
    NamespaceRow namespace =
        context.upsertNamespace(
            Utils.sanitizeOpenLineageNamespace(event.getJob().getNamespace()), now);

    JobProjection jobProjection =
        buildJobFromEvent(
            event.getJob(),
            event.getInputs(),
            transaction.mapper,
            daos,
            now,
            namespace,
            Optional.empty(),
            order);
    JobRow job = jobProjection.job();
    namespace = canonicalNamespaceFor(transaction, namespace, job);
    bag.setNamespace(namespace);
    bag.setJob(job);
    boolean projectCurrentIo = daos.getJobDao().canProjectCurrentIo(job.getUuid(), order);

    Optional<List<DatasetRecord>> datasetInputs =
        projectDatasetSide(context, event.getInputs(), now, null, null, true);
    bag.setInputs(datasetInputs);
    context.flushInputMappings();

    Optional<List<DatasetRecord>> datasetOutputs =
        projectDatasetSide(context, event.getOutputs(), now, null, null, false);
    bag.setOutputs(datasetOutputs);
    context.flushDatasetFacets();
    context.flushColumnLineage(now);

    Map<IoType, List<DatasetRecord>> retainedIo =
        datasetInputs.isPresent() && datasetOutputs.isPresent()
            ? Map.of()
            : loadCurrentJobIo(transaction, job.getNamespaceName(), job.getName());
    List<DatasetRecord> versionInputs =
        datasetInputs.orElseGet(() -> retainedIo.getOrDefault(IoType.INPUT, List.of()));
    List<DatasetRecord> versionOutputs =
        datasetOutputs.orElseGet(() -> retainedIo.getOrDefault(IoType.OUTPUT, List.of()));

    BagOfJobVersionInfo bagOfJobVersionInfo =
        daos.getJobVersionDao()
            .upsertRunlessJobVersionInTransaction(
                job,
                namespace,
                versionInputs,
                versionOutputs,
                order,
                projectCurrentIo,
                getJobLocation(event.getJob()));

    // Runless job facets reference the immutable version and therefore follow its upsert.
    Optional.ofNullable(event.getJob().getFacets())
        .ifPresent(
            jobFacet ->
                daos.getJobFacetsDao()
                    .insertJobFacetsFor(
                        job.getUuid(),
                        bagOfJobVersionInfo.getJobVersionRow().getUuid(),
                        now,
                        jobFacet));

    bag.setJobVersionBag(bagOfJobVersionInfo);
    return bag;
  }

  /** Resolves the immutable identities needed to retain unreported runless-job I/O sides. */
  private Map<IoType, List<DatasetRecord>> loadCurrentJobIo(
      ProjectionTransactionContext transaction, String jobNamespace, String jobName) {
    ModelDaos daos = transaction.daos;
    Map<IoType, List<DatasetRecord>> recordsByType = new LinkedHashMap<>();
    for (JobDataset current :
        daos.getJobVersionDao().findCurrentInputOutputDatasetsFor(jobNamespace, jobName)) {
      Optional<DatasetRow> datasetRow =
          daos.getDatasetDao().findDatasetAsRow(current.namespace(), current.name());
      if (datasetRow.isEmpty() || datasetRow.get().getCurrentVersionUuid().isEmpty()) {
        continue;
      }
      DatasetRow dataset = datasetRow.get();
      Optional<DatasetVersionRow> versionRow =
          transaction.findDatasetVersion(dataset.getCurrentVersionUuid().get());
      if (versionRow.isEmpty()) {
        continue;
      }
      Optional<NamespaceRow> namespaceRow = transaction.findNamespace(current.namespace());
      if (namespaceRow.isEmpty()) {
        continue;
      }
      recordsByType
          .computeIfAbsent(current.ioType(), ignored -> new ArrayList<>())
          .add(new DatasetRecord(dataset, versionRow.get(), namespaceRow.get(), List.of()));
    }
    return recordsByType;
  }

  private LineageProjectionResult updateBaseMarquezModel(
      LineageEvent event, ObjectMapper mapper, Instant now, LineageWriteContext context) {
    ModelDaos daos = context.daos;
    UpdateLineageRow bag = new UpdateLineageRow();
    NamespaceRow namespace =
        context.upsertNamespace(
            Utils.sanitizeOpenLineageNamespace(event.getJob().getNamespace()), now);

    RunFacet runFacets = event.getRun().getFacets();
    NominalTimeRunFacet nominalTime = runFacets == null ? null : runFacets.getNominalTime();
    Instant nominalStartTime =
        toInstant(nominalTime == null ? null : nominalTime.getNominalStartTime());
    Instant nominalEndTime =
        toInstant(nominalTime == null ? null : nominalTime.getNominalEndTime());
    Optional<ParentRunFacet> parentRun = Optional.ofNullable(runFacets).map(RunFacet::getParent);

    JobProjection jobProjection =
        buildJobFromEvent(
            event.getJob(),
            event.getInputs(),
            mapper,
            daos,
            now,
            namespace,
            parentRun,
            context.order);
    JobRow job = jobProjection.job();
    namespace = canonicalNamespaceFor(context.transaction, namespace, job);
    bag.setNamespace(namespace);
    bag.setJob(job);
    boolean projectCurrentIo = daos.getJobDao().canProjectCurrentIo(job.getUuid(), context.order);

    SerializedRunArgs serializedRunArgs = serializeRunArgs(runFacets);
    RunArgsRow runArgs =
        daos.getRunArgsDao()
            .upsertRunArgs(
                UUID.randomUUID(), now, serializedRunArgs.json(), serializedRunArgs.checksum());
    bag.setRunArgs(runArgs);

    RunUpsert.RunUpsertBuilder runUpsertBuilder =
        RunUpsert.builder()
            .runUuid(Utils.openLineageRunUuid(event.getRun().getRunId()))
            .parentRunUuid(jobProjection.effectiveParentRunUuid())
            .externalId(event.getRun().getRunId())
            .now(now)
            .jobUuid(job.getUuid())
            .jobVersionUuid(null)
            .runArgsUuid(runArgs.getUuid())
            .nominalStartTime(nominalStartTime)
            .nominalEndTime(nominalEndTime)
            .namespaceName(job.getNamespaceName())
            .jobName(job.getName())
            .location(getJobLocation(event.getJob()));
    String eventType = event.getEventType();
    RunState eventRunState = eventType == null ? null : getRunState(eventType);
    if (eventRunState != null) {
      runUpsertBuilder.runStateType(eventRunState).runStateTime(now);
    }
    RunRow run = daos.getRunDao().upsertOpenLineageRun(runUpsertBuilder.build());
    UUID runUuid = run.getUuid();
    context.bindRunUuid(runUuid);
    daos.getJobDao().updateCurrentRunFor(job.getUuid(), runUuid, context.order);
    if (runFacets != null) {
      daos.getRunFacetsDao().insertRunFacetsFor(runUuid, now, eventType, runFacets);
    }
    bag.setRun(run);

    if (eventRunState != null) {
      RunStateRow runState =
          daos.getRunStateDao()
              .insertAndLinkRunState(UUID.randomUUID(), now, run.getUuid(), eventRunState);
      bag.setRunState(runState);
    }

    // These rows must exist before a terminal transition links them to a job version.
    JobFacet jobFacets = event.getJob().getFacets();
    if (jobFacets != null) {
      daos.getJobFacetsDao().insertJobFacetsFor(job.getUuid(), runUuid, now, eventType, jobFacets);
    }

    // A null list remains the sentinel for a run event that did not report this side of its I/O.
    Optional<List<DatasetRecord>> datasetInputs =
        projectRunDatasetSide(
            context,
            event.getInputs(),
            now,
            runUuid,
            eventType,
            IoType.INPUT,
            job.getUuid(),
            projectCurrentIo);
    bag.setInputs(datasetInputs);

    // Column lineage resolves through runs_input_mapping, so publish every input before outputs.
    context.flushInputMappings();

    Optional<List<DatasetRecord>> datasetOutputs =
        projectRunDatasetSide(
            context,
            event.getOutputs(),
            now,
            runUuid,
            eventType,
            IoType.OUTPUT,
            job.getUuid(),
            projectCurrentIo);
    bag.setOutputs(datasetOutputs);

    context.flushDatasetFacets();
    context.flushColumnLineage(now);
    return new LineageProjectionResult(bag, projectCurrentIo);
  }

  private Optional<List<DatasetRecord>> projectRunDatasetSide(
      LineageWriteContext context,
      @Nullable List<Dataset> datasets,
      Instant now,
      UUID runUuid,
      @Nullable String eventType,
      IoType ioType,
      UUID jobUuid,
      boolean projectCurrentIo) {
    Optional<List<DatasetRecord>> records =
        projectDatasetSide(context, datasets, now, runUuid, eventType, ioType == IoType.INPUT);
    if (projectCurrentIo && records.filter(List::isEmpty).isPresent()) {
      context.daos.getJobVersionDao().markInputOrOutputDatasetAsPreviousFor(jobUuid, ioType);
    }
    return records;
  }

  private Optional<List<DatasetRecord>> projectDatasetSide(
      LineageWriteContext context,
      @Nullable List<Dataset> datasets,
      Instant now,
      @Nullable UUID runUuid,
      @Nullable String eventType,
      boolean input) {
    return Optional.ofNullable(datasets)
        .map(
            reported ->
                upsertAndQueueLineageDatasets(context, reported, now, runUuid, eventType, input));
  }

  private static @Nullable Instant toInstant(@Nullable ZonedDateTime dateTime) {
    return dateTime == null ? null : dateTime.toInstant();
  }

  private JobProjection buildJobFromEvent(
      Job job,
      List<Dataset> inputs,
      ObjectMapper mapper,
      ModelDaos daos,
      Instant now,
      NamespaceRow namespace,
      Optional<ParentRunFacet> parentRun,
      ProjectionOrder order) {
    JobDao jobDao = daos.getJobDao();
    String description =
        Optional.ofNullable(job.getFacets())
            .map(JobFacet::getDocumentation)
            .map(DocumentationJobFacet::getDescription)
            .orElse(null);

    String location = getJobLocation(job);

    Optional<ParentJobResolution> parentResolution =
        parentRun.map(facet -> findParentJobRow(daos, job, facet, order));
    Optional<JobRow> parentJob = parentResolution.map(ParentJobResolution::job);

    // construct the simple name of the job by removing the parent prefix plus the dot '.' separator
    String jobName =
        parentResolution
            .map(
                parent -> {
                  String reportedParentName = parent.reportedJobName();
                  return job.getName().startsWith(reportedParentName + '.')
                      ? job.getName().substring(reportedParentName.length() + 1)
                      : job.getName();
                })
            .orElse(job.getName());
    LOG.debug(
        "Calculated job name {} from job {} with parent {}",
        jobName,
        job.getName(),
        parentJob.map(JobRow::getName));
    PGobject currentInputs = inputs == null ? null : jobDao.toJson(toDatasetId(inputs), mapper);
    JobRow upsertedJob =
        jobDao.upsertJob(
            JobDao.JobUpsertRequest.forOpenLineageProjection(
                UUID.randomUUID(),
                parentJob.map(JobRow::getUuid).orElse(null),
                job.type(),
                now,
                namespace.getUuid(),
                namespace.getName(),
                jobName,
                description,
                location,
                null,
                currentInputs,
                order));
    String requestedFullName =
        parentJob.map(parent -> parent.getName() + "." + jobName).orElse(jobName);
    JobRow canonicalJob =
        lockCanonicalJobIfAliased(
            jobDao,
            upsertedJob,
            namespace.getName(),
            requestedFullName,
            job.type(),
            now,
            description,
            location,
            currentInputs,
            order);
    return new JobProjection(
        canonicalJob, parentResolution.map(ParentJobResolution::effectiveRunUuid).orElse(null));
  }

  private JobRow lockCanonicalJobIfAliased(
      JobDao jobDao,
      JobRow upsertedJob,
      String requestedNamespace,
      String requestedFullName,
      JobType requestedType,
      Instant updatedAt,
      @Nullable String description,
      @Nullable String location,
      @Nullable PGobject inputs,
      ProjectionOrder order) {
    if (requestedNamespace.equals(upsertedJob.getNamespaceName())
        && requestedFullName.equals(upsertedJob.getName())) {
      // INSERT ... ON CONFLICT already holds this primary row's lock.
      return upsertedJob;
    }
    JobRow canonical = jobDao.lockJobByUuid(upsertedJob.getUuid());
    jobDao.projectOpenLineageSnapshotForCanonicalAlias(
        canonical.getUuid(), requestedType, updatedAt, description, location, inputs, order);
    return jobDao.lockJobByUuid(canonical.getUuid());
  }

  private ParentJobResolution findParentJobRow(
      ModelDaos daos, Job job, ParentRunFacet facet, ProjectionOrder order) {
    try {
      LOG.debug("Found parent run event {}", facet);
      String parentNamespaceName =
          Utils.sanitizeOpenLineageNamespace(facet.getJob().getNamespace());
      ParentJobResolution parentResolution =
          createParentJobRunRecord(daos, job, parentNamespaceName, facet, order);
      LOG.debug("Found parent job record {}", parentResolution.job());
      return parentResolution;
    } catch (Exception e) {
      throw new RuntimeException("Unable to insert parent run", e);
    }
  }

  private ParentJobResolution createParentJobRunRecord(
      ModelDaos daos,
      Job job,
      String parentNamespaceName,
      ParentRunFacet facet,
      ProjectionOrder order) {
    String parentJobName = reportedParentJobName(job, facet);
    NamespaceRow parentNamespace =
        daos.getNamespaceDao()
            .upsertNamespaceRow(
                UUID.randomUUID(), Instant.EPOCH, parentNamespaceName, DEFAULT_NAMESPACE_OWNER);
    JobRow parentJob =
        daos.getJobDao()
            .getOrCreateSyntheticParentJob(
                UUID.randomUUID(),
                parentNamespace.getUuid(),
                parentNamespace.getName(),
                parentJobName);
    LOG.info("Resolved parent job record {}", parentJob);
    UUID parentRunUuid = preferredParentRunUuid(job, facet, parentJob);

    RunArgsRow argsRow =
        daos.getRunArgsDao()
            .upsertRunArgs(
                UUID.randomUUID(), Instant.EPOCH, EMPTY_RUN_ARGS_JSON, EMPTY_RUN_ARGS_CHECKSUM);
    RunDao runDao = daos.getRunDao();
    RunRow parentRun =
        runDao.getOrCreateSyntheticParentRun(
            parentRunUuid,
            facet.getRun().getRunId(),
            parentJob.getUuid(),
            argsRow.getUuid(),
            parentJob.getNamespaceName(),
            parentJob.getName());
    LOG.info("Resolved parent run record {}", parentRun);
    daos.getJobDao().updateCurrentRunFor(parentJob.getUuid(), parentRun.getUuid(), order);
    return new ParentJobResolution(parentJob, parentRun.getUuid(), parentJobName);
  }

  private static String reportedParentJobName(Job childJob, ParentRunFacet facet) {
    String reportedParentName = facet.getJob().getName();
    return reportedParentName.equals(childJob.getName())
        ? Utils.parseParentJobName(reportedParentName)
        : reportedParentName;
  }

  private static UUID preferredParentRunUuid(
      Job childJob, ParentRunFacet facet, JobRow canonicalParentJob) {
    String externalId = facet.getRun().getRunId();
    UUID normalRunUuid = Utils.openLineageRunUuid(externalId);
    UUID reportedPreferredRunUuid = Utils.openLineageParentRunUuid(facet, childJob.getName());
    return reportedPreferredRunUuid.equals(normalRunUuid)
        ? normalRunUuid
        : Utils.toNameBasedUuid(
            canonicalParentJob.getNamespaceName(), canonicalParentJob.getName(), externalId);
  }

  private static NamespaceRow canonicalNamespaceFor(
      ProjectionTransactionContext transaction,
      NamespaceRow requestedNamespace,
      JobRow canonicalJob) {
    UUID canonicalNamespaceUuid = canonicalJob.getNamespaceUuid();
    if (requestedNamespace.getName().equals(canonicalJob.getNamespaceName())
        && (canonicalNamespaceUuid == null
            || requestedNamespace.getUuid().equals(canonicalNamespaceUuid))) {
      return requestedNamespace;
    }
    NamespaceRow canonicalNamespace =
        transaction
            .findNamespace(canonicalJob.getNamespaceName())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Canonical job namespace does not exist: "
                            + canonicalJob.getNamespaceName()));
    if (canonicalNamespaceUuid != null
        && !canonicalNamespace.getUuid().equals(canonicalNamespaceUuid)) {
      throw new IllegalStateException(
          "Canonical job namespace UUID does not match its namespace row: "
              + canonicalJob.getUuid());
    }
    return canonicalNamespace;
  }

  private static @Nullable String getJobLocation(Job job) {
    return Optional.ofNullable(job.getFacets())
        .map(JobFacet::getSourceCodeLocation)
        .map(source -> source.getUrl())
        .orElse(null);
  }

  private Set<DatasetId> toDatasetId(List<Dataset> datasets) {
    Set<DatasetId> ids = new HashSet<>();
    if (datasets != null) {
      for (Dataset dataset : datasets) {
        ids.add(
            new DatasetId(
                NamespaceName.of(dataset.getNamespace()), DatasetName.of(dataset.getName())));
      }
    }
    return ids;
  }

  private static List<SymlinkIdentifier> symlinkIdentifiers(Dataset dataset) {
    return Optional.ofNullable(dataset.getFacets())
        .map(DatasetFacets::getSymlinks)
        .map(LineageEvent.DatasetSymlinkFacet::getIdentifiers)
        .orElseGet(List::of);
  }

  private void updateRunJobVersionInTransaction(
      LineageEvent event,
      Instant transitionedAt,
      LineageProjectionResult projection,
      RunState runState,
      ModelDaos daos,
      @Nullable RunIoSnapshot runIoSnapshot,
      ProjectionOrder order) {
    boolean streaming = event.getJob() != null && event.getJob().isStreamingJob();
    if (runIoSnapshot == null) {
      throw new IllegalStateException(
          streaming
              ? "A streaming run event requires a cumulative I/O snapshot"
              : "A terminal run event requires a cumulative I/O snapshot");
    }
    UpdateLineageRow updateLineageRow = projection.row();
    JobVersionDao jobVersionDao = daos.getJobVersionDao();
    JobRowRunDetails jobRowRunDetails =
        jobVersionDao.loadJobRowRunDetails(
            updateLineageRow.getJob(),
            updateLineageRow.getNamespace(),
            updateLineageRow.getRun().getUuid(),
            runIoSnapshot,
            getJobLocation(event.getJob()));

    if (streaming
        && ((event.isTerminalEventForStreamingJobWithNoDatasets()
                && event.getInputs() == null
                && event.getOutputs() == null)
            || (jobVersionDao.versionExists(jobRowRunDetails.jobVersion().getValue())
                && !projection.projectCurrentIo()))) {
      hydrateSkippedRunJobVersionProjection(daos, updateLineageRow, runIoSnapshot, order);
      return;
    }

    @Nullable
    BagOfJobVersionInfo bagOfJobVersionInfo =
        jobVersionDao.upsertJobVersionOnRunTransitionInTransaction(
            jobRowRunDetails, runState, transitionedAt, true, order, projection.projectCurrentIo());
    if (bagOfJobVersionInfo != null) {
      updateLineageRow.setJobVersionBag(bagOfJobVersionInfo);
    }
  }

  private static void hydrateSkippedRunJobVersionProjection(
      ModelDaos daos, UpdateLineageRow row, RunIoSnapshot runIoSnapshot, ProjectionOrder order) {
    UUID runUuid = row.getRun().getUuid();
    if (!daos.getRunDao().claimOpenLineageJobVersionProjection(runUuid, order)) {
      return;
    }
    daos.getJobVersionDao()
        .findJobVersionLinkedToRun(runUuid)
        .ifPresent(
            jobVersion ->
                row.setJobVersionBag(
                    new BagOfJobVersionInfo(
                        row.getJob(),
                        jobVersion,
                        runIoSnapshot.getInputs(),
                        runIoSnapshot.getOutputs())));
  }

  private void updateOutputDatasetVersions(
      ModelDaos daos,
      RunIoSnapshot runIoSnapshot,
      List<DatasetRecord> reportedOutputs,
      Instant now,
      ProjectionOrder order) {
    Map<UUID, UUID> selectedVersionByDataset = new LinkedHashMap<>();
    Set<UUID> ambiguousDatasets = new LinkedHashSet<>();
    for (var output : runIoSnapshot.getOutputs()) {
      UUID previous =
          selectedVersionByDataset.putIfAbsent(output.getDatasetUuid(), output.getUuid());
      if (previous != null && !previous.equals(output.getUuid())) {
        selectedVersionByDataset.remove(output.getDatasetUuid());
        ambiguousDatasets.add(output.getDatasetUuid());
      }
    }

    // A cumulative run snapshot cannot reconstruct occurrence order for an older event. Preserve
    // the existing pointer for those ambiguous datasets, and overlay this payload's known last
    // occurrence in encounter order.
    for (DatasetRecord output : reportedOutputs) {
      UUID datasetUuid = output.getDatasetRow().getUuid();
      ambiguousDatasets.remove(datasetUuid);
      selectedVersionByDataset.put(datasetUuid, output.getDatasetVersionRow().getUuid());
    }

    List<DatasetCurrentVersionUpdate> updates = new ArrayList<>(selectedVersionByDataset.size());
    for (Map.Entry<UUID, UUID> selected : selectedVersionByDataset.entrySet()) {
      if (!ambiguousDatasets.contains(selected.getKey())) {
        updates.add(new DatasetCurrentVersionUpdate(selected.getKey(), now, selected.getValue()));
      }
    }
    if (!updates.isEmpty()) {
      daos.getDatasetDao().updateVersionsInTransaction(updates, order);
    }
  }

  /** Event projection plus the single shared current-I/O winner decision for that event. */
  private record LineageProjectionResult(UpdateLineageRow row, boolean projectCurrentIo) {}

  /** Canonical job plus the parent run UUID actually persisted after collision repair. */
  private record JobProjection(JobRow job, @Nullable UUID effectiveParentRunUuid) {}

  private record ParentJobResolution(JobRow job, UUID effectiveRunUuid, String reportedJobName) {}

  private record SerializedRunArgs(String json, String checksum) {
    private static final SerializedRunArgs EMPTY =
        new SerializedRunArgs(EMPTY_RUN_ARGS_JSON, EMPTY_RUN_ARGS_CHECKSUM);
  }

  private String getUrlOrNull(String uri) {
    if (uri == null) {
      return "";
    }
    try {
      return new URI(uri).toASCIIString();
    } catch (URISyntaxException e) {
      try {
        // assume host as string
        return new URI("http://" + uri).toASCIIString();
      } catch (Exception ex) {
        return null;
      }
    }
  }

  private List<DatasetRecord> upsertAndQueueLineageDatasets(
      LineageWriteContext context,
      List<Dataset> datasets,
      Instant eventTime,
      @Nullable UUID runUuid,
      @Nullable String eventType,
      boolean isInput) {
    List<DatasetRecord> records =
        datasets.isEmpty()
            ? List.of()
            : upsertLineageDatasetsInPayloadOrder(context, datasets, eventTime, runUuid, isInput);
    for (int index = 0; index < datasets.size(); index++) {
      context.queueDatasetFacets(
          datasets.get(index), records.get(index), runUuid, eventType, eventTime, isInput);
    }
    return records;
  }

  private List<DatasetRecord> upsertLineageDatasetsInPayloadOrder(
      LineageWriteContext context,
      List<Dataset> datasets,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    // Namespace and source resolution intentionally stays in encounter order. Only the primary
    // symlink resolution and dataset upserts are staged across this one I/O side.
    List<LineageWriteContext.PreparedLineageDatasetBase> preparedBases =
        new ArrayList<>(datasets.size());
    for (Dataset dataset : datasets) {
      LineageWriteContext.PreparedLineageDatasetBase preparedBase =
          prepareLineageDatasetBase(context, dataset, now);
      preparedBases.add(preparedBase);
    }

    List<DatasetSymlinkRow> symlinks = new ArrayList<>(preparedBases.size());
    for (LineageWriteContext.PreparedLineageDatasetBase preparedBase : preparedBases) {
      DatasetSymlinkRow planned = context.plannedSymlinkFor(preparedBase);
      materializeGuardedRawAliases(context, preparedBase, planned, now);
      symlinks.add(planned);
    }
    Set<UUID> datasetUuids = new LinkedHashSet<>();
    boolean repeatedDatasetUuid = false;
    for (DatasetSymlinkRow symlink : symlinks) {
      repeatedDatasetUuid |= !datasetUuids.add(symlink.getUuid());
    }

    // DatasetDao maps every occurrence back to payload order. Ordered duplicates share the one
    // canonical row produced from the last occurrence.
    List<LineageWriteContext.PreparedLineageDataset> prepared =
        upsertPreparedDatasetBases(context, preparedBases, symlinks, now);

    if (repeatedDatasetUuid) {
      // Distinct primary names can still be aliases for one physical dataset. Finish immutable
      // occurrence work sequentially while carrying input current-version state forward.
      return upsertPreparedLineageDatasetsSequentially(context, prepared, now, runUuid, isInput);
    }

    List<DatasetFieldUpsert> fieldUpserts = new ArrayList<>();
    for (LineageWriteContext.PreparedLineageDataset occurrence : prepared) {
      fieldUpserts.addAll(toDatasetFieldUpserts(occurrence, now));
    }
    List<DatasetFieldRow> allDatasetFields =
        context.daos.getDatasetFieldDao().upsertAllInTransaction(fieldUpserts);

    Map<UUID, DatasetVersionRow> currentInputVersions = new LinkedHashMap<>();
    if (isInput) {
      Set<UUID> currentVersionUuids = new LinkedHashSet<>();
      for (LineageWriteContext.PreparedLineageDataset occurrence : prepared) {
        occurrence.datasetRow().getCurrentVersionUuid().ifPresent(currentVersionUuids::add);
      }
      for (DatasetVersionRow versionRow :
          context.daos.getDatasetVersionDao().findRowsByUuids(currentVersionUuids)) {
        currentInputVersions.put(versionRow.getUuid(), versionRow);
        context.cacheDatasetVersion(versionRow);
      }
    }

    List<DatasetRecord> records = new ArrayList<>(prepared.size());
    List<DatasetFieldMapping> fieldMappings = new ArrayList<>(allDatasetFields.size());
    List<DatasetCurrentVersionUpdate> currentVersionUpdates = new ArrayList<>();
    int fieldOffset = 0;
    for (int index = 0; index < prepared.size(); index++) {
      LineageWriteContext.PreparedLineageDataset occurrence = prepared.get(index);
      int nextFieldOffset = fieldOffset + occurrence.fieldsOrEmpty().size();
      List<DatasetFieldRow> datasetFields = allDatasetFields.subList(fieldOffset, nextFieldOffset);
      fieldOffset = nextFieldOffset;
      DatasetRow datasetRow = occurrence.datasetRow();
      DatasetVersionRow datasetVersionRow =
          isInput
              ? datasetRow.getCurrentVersionUuid().map(currentInputVersions::get).orElse(null)
              : null;
      if (datasetVersionRow == null) {
        datasetVersionRow =
            createDatasetVersion(context, occurrence, datasetFields, now, runUuid, isInput);
      }

      fieldMappings.addAll(toDatasetFieldMappings(datasetVersionRow, datasetFields));

      if (isInput && runUuid != null) {
        context.queueInputMapping(datasetVersionRow.getUuid());
        if (datasetRow.getCurrentVersionUuid().isEmpty()) {
          currentVersionUpdates.add(
              new DatasetCurrentVersionUpdate(
                  datasetRow.getUuid(), now, datasetVersionRow.getUuid()));
          datasetRow = datasetRow.withCurrentVersionUuid(datasetVersionRow.getUuid());
        }
      }

      if (!isInput) {
        // For run intake, input mappings are already visible. Physical preparation only queues
        // UUIDs; the output mappings below still precede the event-level column-lineage flush.
        context.collectColumnLineage(occurrence.base().dataset(), datasetFields, datasetVersionRow);
      }
      records.add(
          new DatasetRecord(
              datasetRow, datasetVersionRow, occurrence.base().datasetNamespace(), List.of()));
    }

    // The caller publishes run input mappings only after the input side returns. Output physical
    // preparation above may resolve those inputs, but the lineage write is flushed still later,
    // after this output mapping and the facets.
    context.daos.getDatasetFieldDao().updateFieldMappingInTransaction(fieldMappings);
    context.daos.getDatasetDao().updateVersionsInTransaction(currentVersionUpdates, context.order);
    return records;
  }

  private static List<DatasetFieldUpsert> toDatasetFieldUpserts(
      LineageWriteContext.PreparedLineageDataset occurrence, Instant now) {
    return occurrence.fieldsOrEmpty().stream()
        .map(
            field ->
                new DatasetFieldUpsert(
                    UUID.randomUUID(),
                    now,
                    now,
                    occurrence.datasetRow().getUuid(),
                    field.getName(),
                    field.getType(),
                    field.getDescription()))
        .toList();
  }

  private static List<DatasetFieldMapping> toDatasetFieldMappings(
      DatasetVersionRow version, List<DatasetFieldRow> fields) {
    return fields.stream()
        .map(field -> new DatasetFieldMapping(version.getUuid(), field.getUuid()))
        .toList();
  }

  private List<LineageWriteContext.PreparedLineageDataset> upsertPreparedDatasetBases(
      LineageWriteContext context,
      List<LineageWriteContext.PreparedLineageDatasetBase> preparedBases,
      List<DatasetSymlinkRow> symlinks,
      Instant now) {
    List<DatasetUpsert> datasetUpserts = new ArrayList<>(preparedBases.size());
    for (int index = 0; index < preparedBases.size(); index++) {
      datasetUpserts.add(toDatasetUpsert(preparedBases.get(index), symlinks.get(index), now));
    }

    List<DatasetRow> datasetRows =
        context.daos.getDatasetDao().upsertAllInTransaction(datasetUpserts, context.order);
    List<LineageWriteContext.PreparedLineageDataset> prepared =
        new ArrayList<>(preparedBases.size());
    for (int index = 0; index < preparedBases.size(); index++) {
      prepared.add(
          new LineageWriteContext.PreparedLineageDataset(
              preparedBases.get(index), symlinks.get(index), datasetRows.get(index)));
    }
    return prepared;
  }

  private List<DatasetRecord> upsertPreparedLineageDatasetsSequentially(
      LineageWriteContext context,
      List<LineageWriteContext.PreparedLineageDataset> prepared,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    List<DatasetRecord> records = new ArrayList<>(prepared.size());
    Map<UUID, UUID> currentVersionByDataset = new LinkedHashMap<>();
    for (LineageWriteContext.PreparedLineageDataset occurrence : prepared) {
      DatasetRow datasetRow = occurrence.datasetRow();
      UUID carriedCurrentVersion = currentVersionByDataset.get(datasetRow.getUuid());
      if (carriedCurrentVersion != null) {
        occurrence =
            occurrence.withDatasetRow(datasetRow.withCurrentVersionUuid(carriedCurrentVersion));
      }
      DatasetRecord record =
          upsertPreparedLineageDatasetSequentially(context, occurrence, now, runUuid, isInput);
      records.add(record);
      record
          .getDatasetRow()
          .getCurrentVersionUuid()
          .ifPresent(
              currentVersion ->
                  currentVersionByDataset.put(record.getDatasetRow().getUuid(), currentVersion));
    }
    return records;
  }

  private DatasetRecord upsertPreparedLineageDatasetSequentially(
      LineageWriteContext context,
      LineageWriteContext.PreparedLineageDataset occurrence,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    ModelDaos daos = context.daos;
    List<DatasetFieldUpsert> fieldUpserts = toDatasetFieldUpserts(occurrence, now);
    List<DatasetFieldRow> datasetFields =
        fieldUpserts.isEmpty()
            ? List.of()
            : daos.getDatasetFieldDao().upsertAllInTransaction(fieldUpserts);

    DatasetRow datasetRow = occurrence.datasetRow();
    DatasetVersionRow datasetVersionRow =
        datasetRow
            .getCurrentVersionUuid()
            .filter(ignored -> isInput)
            .flatMap(context::findDatasetVersion)
            .orElseGet(
                () ->
                    createDatasetVersion(
                        context, occurrence, datasetFields, now, runUuid, isInput));

    List<DatasetFieldMapping> fieldMappings =
        toDatasetFieldMappings(datasetVersionRow, datasetFields);
    if (!fieldMappings.isEmpty()) {
      daos.getDatasetFieldDao().updateFieldMappingInTransaction(fieldMappings);
    }

    if (isInput && runUuid != null) {
      context.queueInputMapping(datasetVersionRow.getUuid());

      // TODO - this is a short term fix until
      // https://github.com/MarquezProject/marquez/issues/1361 is fully thought out
      if (datasetRow.getCurrentVersionUuid().isEmpty()) {
        daos.getDatasetDao()
            .updateVersionsInTransaction(
                List.of(
                    new DatasetCurrentVersionUpdate(
                        datasetRow.getUuid(), now, datasetVersionRow.getUuid())),
                context.order);
        datasetRow = datasetRow.withCurrentVersionUuid(datasetVersionRow.getUuid());
      }
    }

    if (!isInput) {
      context.collectColumnLineage(occurrence.base().dataset(), datasetFields, datasetVersionRow);
    }
    return new DatasetRecord(
        datasetRow, datasetVersionRow, occurrence.base().datasetNamespace(), List.of());
  }

  private void materializeGuardedRawAliases(
      LineageWriteContext context,
      LineageWriteContext.PreparedLineageDatasetBase preparedBase,
      DatasetSymlinkRow canonical,
      Instant now) {
    if (!preparedBase.rawNamespace().getUuid().equals(preparedBase.datasetNamespace().getUuid())) {
      context
          .daos
          .getDatasetSymlinkDao()
          .upsertOpenLineageRawAlias(
              canonical.getUuid(),
              preparedBase.primaryName(),
              preparedBase.rawNamespace().getUuid(),
              now);
    }
    for (SymlinkIdentifier alias : symlinkIdentifiers(preparedBase.dataset())) {
      String normalizedNamespace = Utils.sanitizeOpenLineageNamespace(alias.getNamespace());
      if (!alias.getNamespace().equals(normalizedNamespace)) {
        context
            .daos
            .getDatasetSymlinkDao()
            .upsertOpenLineageRawAlias(
                canonical.getUuid(),
                alias.getName(),
                context.upsertNamespace(alias.getNamespace(), now).getUuid(),
                now);
      }
    }
  }

  private LineageWriteContext.PreparedLineageDatasetBase prepareLineageDatasetBase(
      LineageWriteContext context, Dataset ds, Instant now) {
    String rawNamespaceName = ds.getNamespace();
    String formattedNamespaceName = Utils.sanitizeOpenLineageNamespace(rawNamespaceName);
    NamespaceRow dsNamespace = context.upsertNamespace(rawNamespaceName, now);

    DatasetFacets facets = ds.getFacets();
    boolean hasExplicitSource = facets != null && facets.getDataSource() != null;
    String sourceName = hasExplicitSource ? facets.getDataSource().getName() : DEFAULT_SOURCE_NAME;
    String sourceUrl = hasExplicitSource ? getUrlOrNull(facets.getDataSource().getUri()) : "";
    SourceRow source =
        context.upsertSource(
            sourceName, OPEN_LINEAGE_SOURCE_TYPE, sourceUrl, hasExplicitSource, now);

    String dsDescription =
        facets != null && facets.getDocumentation() != null
            ? facets.getDocumentation().getDescription()
            : null;
    String lifecycleState =
        Optional.ofNullable(facets)
            .map(DatasetFacets::getLifecycleStateChange)
            .map(LifecycleStateChangeFacet::getLifecycleStateChange)
            .orElse("");
    List<SchemaField> fields =
        Optional.ofNullable(facets)
            .map(DatasetFacets::getSchema)
            .map(SchemaDatasetFacet::getFields)
            .orElse(null);

    NamespaceRow datasetNamespace =
        rawNamespaceName.equals(formattedNamespaceName)
            ? dsNamespace
            : context.upsertNamespace(formattedNamespaceName, now);

    return new LineageWriteContext.PreparedLineageDatasetBase(
        ds,
        dsNamespace,
        datasetNamespace,
        source,
        dsDescription,
        lifecycleState,
        fields,
        ds.getName());
  }

  private DatasetUpsert toDatasetUpsert(
      LineageWriteContext.PreparedLineageDatasetBase preparedBase,
      DatasetSymlinkRow symlink,
      Instant now) {
    Dataset ds = preparedBase.dataset();
    return new DatasetUpsert(
        symlink.getUuid(),
        OPEN_LINEAGE_DATASET_TYPE,
        now,
        preparedBase.datasetNamespace().getUuid(),
        preparedBase.datasetNamespace().getName(),
        preparedBase.source().getUuid(),
        preparedBase.source().getName(),
        preparedBase.primaryName(),
        ds.getName(),
        preparedBase.description(),
        preparedBase.lifecycleState().equalsIgnoreCase("DROP"));
  }

  private DatasetVersionRow createDatasetVersion(
      LineageWriteContext context,
      LineageWriteContext.PreparedLineageDataset occurrence,
      List<DatasetFieldRow> datasetFields,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    ModelDaos daos = context.daos;
    DatasetRow datasetRow = occurrence.datasetRow();
    LineageWriteContext.PreparedLineageDatasetBase base = occurrence.base();
    UUID identityRunUuid = isInput ? null : runUuid;
    UUID versionUuid =
        Utils.newDatasetVersionFor(
                base.rawNamespace().getName(),
                base.source().getName(),
                base.dataset().getName(),
                occurrence.symlink().getName(),
                base.lifecycleState(),
                base.fields(),
                identityRunUuid)
            .getValue();
    UUID datasetSchemaVersionUuid =
        daos.getDatasetSchemaVersionDao()
            .upsertSchemaVersionInTransaction(datasetRow, datasetFields, now)
            .getValue();
    DatasetVersionRow version =
        daos.getDatasetVersionDao()
            .upsert(
                UUID.randomUUID(),
                now,
                datasetRow.getUuid(),
                versionUuid,
                datasetSchemaVersionUuid,
                isInput ? null : runUuid,
                daos.getDatasetVersionDao().toPgObjectSchemaFields(base.fields()),
                base.rawNamespace().getName(),
                base.dataset().getName(),
                base.lifecycleState());
    context.cacheDatasetVersion(version);
    return version;
  }

  /** Mutable state whose lifetime is exactly one relational event projection. */
  private static final class LineageWriteContext {
    private record PreparedLineageDatasetBase(
        Dataset dataset,
        NamespaceRow rawNamespace,
        NamespaceRow datasetNamespace,
        SourceRow source,
        @Nullable String description,
        String lifecycleState,
        @Nullable List<SchemaField> fields,
        String primaryName) {}

    private record PreparedLineageDataset(
        PreparedLineageDatasetBase base, DatasetSymlinkRow symlink, DatasetRow datasetRow) {
      List<SchemaField> fieldsOrEmpty() {
        return base.fields() == null ? List.of() : base.fields();
      }

      PreparedLineageDataset withDatasetRow(DatasetRow replacement) {
        return new PreparedLineageDataset(base, symlink, replacement);
      }
    }

    private final ProjectionTransactionContext transaction;
    private final ModelDaos daos;
    @Nullable private UUID runUuid;
    private final ProjectionOrder order;
    @Nullable private ColumnLineageContext columnLineageContext;
    private final Map<String, SourceState> lastSourceStateByName = new LinkedHashMap<>();
    private final Set<UUID> pendingInputMappings = new LinkedHashSet<>();
    @Nullable private List<DatasetFacetWrite> pendingDatasetFacets;
    private final List<ColumnLineageDatasetWrite> pendingColumnLineage = new ArrayList<>();

    private LineageWriteContext(ProjectionTransactionContext transaction, ProjectionOrder order) {
      this.transaction = transaction;
      this.daos = transaction.daos;
      this.order = Objects.requireNonNull(order, "order");
    }

    Optional<DatasetVersionRow> findDatasetVersion(UUID versionUuid) {
      return transaction.findDatasetVersion(versionUuid);
    }

    void cacheDatasetVersion(DatasetVersionRow version) {
      transaction.cacheDatasetVersion(version);
    }

    void bindRunUuid(UUID effectiveRunUuid) {
      if (runUuid != null && !runUuid.equals(effectiveRunUuid)) {
        throw new IllegalStateException("Lineage write context is already bound to run " + runUuid);
      }
      runUuid = effectiveRunUuid;
      columnLineageContext = new ColumnLineageContext(daos, effectiveRunUuid);
    }

    NamespaceRow upsertNamespace(String exactName, Instant now) {
      return transaction.upsertNamespace(exactName, now);
    }

    DatasetSymlinkRow plannedSymlinkFor(PreparedLineageDatasetBase preparedBase) {
      DatasetSymlinkRow row =
          Objects.requireNonNull(
                  transaction.datasetAliasPlan, "Dataset alias plan is not installed")
              .rowsByIdentity()
              .get(
                  new DatasetIdentity(
                      preparedBase.datasetNamespace().getName(), preparedBase.primaryName()));
      if (row == null) {
        throw new IllegalStateException(
            "Dataset alias plan omitted payload identity "
                + preparedBase.datasetNamespace().getName()
                + "/"
                + preparedBase.primaryName());
      }
      return row;
    }

    SourceRow upsertSource(
        String name, String type, @Nullable String connectionUrl, boolean explicit, Instant now) {
      SourceSpec requested = new SourceSpec(explicit, name, type, connectionUrl);
      SourceState previous = lastSourceStateByName.get(name);
      if (previous != null && previous.spec().equals(requested)) {
        return previous.row();
      }

      SourceRow row =
          explicit
              ? daos.getSourceDao().upsert(UUID.randomUUID(), type, now, name, connectionUrl)
              : daos.getSourceDao()
                  .upsertOrDefaultInTransaction(UUID.randomUUID(), type, now, name, connectionUrl);
      lastSourceStateByName.put(name, new SourceState(requested, row));
      return row;
    }

    void queueInputMapping(UUID datasetVersionUuid) {
      if (runUuid != null) {
        pendingInputMappings.add(datasetVersionUuid);
      }
    }

    void flushInputMappings() {
      if (runUuid != null && !pendingInputMappings.isEmpty()) {
        daos.getRunDao().updateInputMappingsInTransaction(runUuid, pendingInputMappings);
        pendingInputMappings.clear();
      }
    }

    void queueDatasetFacets(
        Dataset dataset,
        DatasetRecord record,
        @Nullable UUID facetRunUuid,
        @Nullable String eventType,
        Instant lineageEventTime,
        boolean isInput) {
      UUID datasetUuid = record.getDatasetRow().getUuid();
      UUID datasetVersionUuid = record.getDatasetVersionRow().getUuid();
      Instant createdAt = Instant.now();
      DatasetFacets facets = dataset.getFacets();
      if (facets != null) {
        addDatasetFacet(
            DatasetFacetWrite.forDatasetFacets(
                createdAt,
                datasetUuid,
                datasetVersionUuid,
                facetRunUuid,
                lineageEventTime,
                eventType,
                facets));
      }
      if (isInput) {
        var inputFacets = dataset.getInputFacets();
        if (inputFacets != null) {
          addDatasetFacet(
              DatasetFacetWrite.forInputFacets(
                  createdAt,
                  datasetUuid,
                  datasetVersionUuid,
                  facetRunUuid,
                  lineageEventTime,
                  eventType,
                  inputFacets));
        }
      } else {
        var outputFacets = dataset.getOutputFacets();
        if (outputFacets != null) {
          addDatasetFacet(
              DatasetFacetWrite.forOutputFacets(
                  createdAt,
                  datasetUuid,
                  datasetVersionUuid,
                  facetRunUuid,
                  lineageEventTime,
                  eventType,
                  outputFacets));
        }
      }
    }

    private void addDatasetFacet(DatasetFacetWrite write) {
      if (FacetUtils.isEmpty(write.getFacets())) {
        return;
      }
      if (pendingDatasetFacets == null) {
        pendingDatasetFacets = new ArrayList<>();
      }
      pendingDatasetFacets.add(write);
      if (pendingDatasetFacets.size() == DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT) {
        flushDatasetFacets();
      }
    }

    void flushDatasetFacets() {
      if (pendingDatasetFacets != null) {
        List<DatasetFacetWrite> writes = pendingDatasetFacets;
        pendingDatasetFacets = null;
        daos.getDatasetFacetsDao().insertDatasetFacetWritesInTransaction(writes);
      }
    }

    void collectColumnLineage(
        Dataset dataset, List<DatasetFieldRow> datasetFields, DatasetVersionRow datasetVersionRow) {
      if (runUuid == null) {
        return;
      }

      ColumnLineageDatasetWrite write =
          Objects.requireNonNull(columnLineageContext)
              .prepareColumnLineage(dataset, datasetFields, datasetVersionRow);
      if (write != null) {
        pendingColumnLineage.add(write);
      }
    }

    void flushColumnLineage(Instant now) {
      if (runUuid != null && !pendingColumnLineage.isEmpty()) {
        daos.getColumnLineageDao()
            .upsertColumnLineageRowsForIntakeInTransaction(List.copyOf(pendingColumnLineage), now);
        pendingColumnLineage.clear();
      }
    }

    private record SourceSpec(
        boolean explicit, String name, String type, @Nullable String connectionUrl) {}

    private record SourceState(SourceSpec spec, SourceRow row) {}
  }

  /** Lazily loads and indexes a run's input fields for all output datasets in one event. */
  private static final class ColumnLineageContext {
    private final ModelDaos daos;
    private final UUID runUuid;
    private Map<InputFieldKey, List<Pair<UUID, UUID>>> inputFieldsByKey;

    ColumnLineageContext(ModelDaos daos, UUID runUuid) {
      this.daos = daos;
      this.runUuid = runUuid;
    }

    @Nullable
    ColumnLineageDatasetWrite prepareColumnLineage(
        Dataset ds, List<DatasetFieldRow> datasetFields, DatasetVersionRow datasetVersionRow) {
      Map<String, LineageEvent.ColumnLineageOutputColumn> columnLineageByOutput =
          Optional.ofNullable(ds.getFacets())
              .map(DatasetFacets::getColumnLineage)
              .map(LineageEvent.ColumnLineageDatasetFacet::getFields)
              .map(LineageEvent.ColumnLineageDatasetFacetFields::getAdditional)
              .orElse(Map.of());
      if (columnLineageByOutput.isEmpty()) {
        return null;
      }

      Map<String, DatasetFieldRow> outputFieldsByName = new LinkedHashMap<>();
      for (DatasetFieldRow datasetField : datasetFields) {
        outputFieldsByName.putIfAbsent(datasetField.getName(), datasetField);
      }

      List<ColumnLineageWrite> writes = new ArrayList<>(columnLineageByOutput.size());
      for (Map.Entry<String, LineageEvent.ColumnLineageOutputColumn> entry :
          columnLineageByOutput.entrySet()) {
        String columnName = entry.getKey();
        LineageEvent.ColumnLineageOutputColumn columnLineage = entry.getValue();
        if (columnLineage == null) {
          continue;
        }

        DatasetFieldRow outputField = outputFieldsByName.get(columnName);
        if (outputField == null) {
          LOG.error(
              "Cannot produce column lineage for missing output field in output dataset: {}",
              columnName);
          continue;
        }

        List<Pair<UUID, UUID>> inputFields = resolveInputFields(columnLineage.getInputFields());
        LOG.debug(
            "Adding column lineage on output field '{}' for dataset version '{}' with input fields: {}",
            outputField.getName(),
            datasetVersionRow.getUuid(),
            inputFields);
        if (!inputFields.isEmpty()) {
          writes.add(
              new ColumnLineageWrite(
                  outputField.getUuid(),
                  inputFields,
                  columnLineage.getTransformationDescription(),
                  columnLineage.getTransformationType()));
        }
      }
      if (writes.isEmpty()) {
        return null;
      }
      return new ColumnLineageDatasetWrite(datasetVersionRow.getUuid(), writes);
    }

    private List<Pair<UUID, UUID>> resolveInputFields(
        List<LineageEvent.ColumnLineageInputField> inputFields) {
      if (runUuid == null || inputFields == null || inputFields.isEmpty()) {
        return List.of();
      }

      Set<InputFieldKey> requestedFields = new LinkedHashSet<>();
      for (LineageEvent.ColumnLineageInputField inputField : inputFields) {
        requestedFields.add(
            new InputFieldKey(
                inputField.getNamespace(), inputField.getName(), inputField.getField()));
      }

      List<Pair<UUID, UUID>> resolvedFields = new ArrayList<>();
      Map<InputFieldKey, List<Pair<UUID, UUID>>> indexedInputFields = inputFieldsByKey();
      for (InputFieldKey requestedField : requestedFields) {
        resolvedFields.addAll(indexedInputFields.getOrDefault(requestedField, List.of()));
      }
      return resolvedFields;
    }

    private Map<InputFieldKey, List<Pair<UUID, UUID>>> inputFieldsByKey() {
      if (inputFieldsByKey == null) {
        List<InputFieldData> runFields =
            daos.getDatasetFieldDao().findInputFieldsDataAssociatedWithRun(runUuid);
        LOG.debug("Found input datasets fields for run '{}': {}", runUuid, runFields);

        inputFieldsByKey = new LinkedHashMap<>();
        for (InputFieldData fieldData : runFields) {
          InputFieldKey key =
              new InputFieldKey(
                  fieldData.getNamespace(), fieldData.getDatasetName(), fieldData.getField());
          inputFieldsByKey
              .computeIfAbsent(key, ignored -> new ArrayList<>())
              .add(Pair.of(fieldData.getDatasetVersionUuid(), fieldData.getDatasetFieldUuid()));
        }
      }
      return inputFieldsByKey;
    }

    private record InputFieldKey(String namespace, String datasetName, String field) {}
  }

  private RunState getRunState(String eventType) {
    return eventType == null
        ? RunState.RUNNING
        : switch (eventType.toLowerCase()) {
          case "complete" -> RunState.COMPLETED;
          case "abort" -> RunState.ABORTED;
          case "fail" -> RunState.FAILED;
          default -> RunState.RUNNING;
        };
  }

  private SerializedRunArgs serializeRunArgs(@Nullable RunFacet facets) {
    if (facets == null || (facets.getNominalTime() == null && facets.getParent() == null)) {
      return SerializedRunArgs.EMPTY;
    }
    Map<String, String> args = new LinkedHashMap<>();
    NominalTimeRunFacet nominalTime = facets.getNominalTime();
    if (nominalTime != null) {
      args.put("nominal_start_time", nominalTime.getNominalStartTime().toString());
      if (nominalTime.getNominalEndTime() != null) {
        args.put("nominal_end_time", nominalTime.getNominalEndTime().toString());
      }
    }
    ParentRunFacet parent = facets.getParent();
    if (parent != null) {
      args.put("run_id", parent.getRun().getRunId());
      args.put("name", parent.getJob().getName());
      args.put("namespace", parent.getJob().getNamespace());
    }
    return new SerializedRunArgs(Utils.toJson(args), Utils.checksumFor(args));
  }
}
