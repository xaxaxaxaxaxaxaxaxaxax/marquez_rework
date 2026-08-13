/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.common;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static marquez.common.models.CommonModelGenerator.newDatasetName;
import static marquez.common.models.CommonModelGenerator.newFieldName;
import static marquez.common.models.CommonModelGenerator.newFieldType;
import static marquez.common.models.CommonModelGenerator.newJobName;
import static marquez.common.models.CommonModelGenerator.newLifecycleState;
import static marquez.common.models.CommonModelGenerator.newNamespaceName;
import static marquez.common.models.CommonModelGenerator.newRunId;
import static marquez.common.models.CommonModelGenerator.newSchemaFields;
import static marquez.common.models.CommonModelGenerator.newSourceName;
import static marquez.service.models.ServiceModelGenerator.newDbTableMeta;
import static marquez.service.models.ServiceModelGenerator.newJobMeta;
import static marquez.service.models.ServiceModelGenerator.newStreamMeta;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.Field;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.SourceName;
import marquez.common.models.Version;
import marquez.service.models.DbTableMeta;
import marquez.service.models.JobMeta;
import marquez.service.models.LineageEvent;
import marquez.service.models.StreamMeta;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Tag("UnitTests")
public class UtilsTest {
  private static final String VALUE = "test";
  private static final Object OBJECT = new Object(VALUE);
  private static final TypeReference<Object> TYPE = new TypeReference<Object>() {};
  private static final String JSON = "{\"value\":\"" + VALUE + "\"}";

  @Test
  public void testToJson() {
    final String actual = Utils.toJson(OBJECT);
    assertThat(actual).isEqualTo(JSON);
  }

  @Test
  public void testToJson_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> Utils.toJson(null));
  }

  @Test
  public void testFromStringJson() {
    final Object actual = Utils.fromJson(JSON, TYPE);
    assertThat(actual).isEqualToComparingFieldByField(OBJECT);
  }

  @Test
  public void testFromISJson() {
    final Object actual =
        Utils.fromJson(this.getClass().getResourceAsStream("/lineage/node.json"), TYPE);
    assertThat(actual).isNotNull();
  }

  @Test
  public void testFromIOExceptionThrownByFromJson() {
    assertThatExceptionOfType(UncheckedIOException.class)
        .isThrownBy(
            () ->
                Utils.fromJson(
                    new ByteArrayInputStream(JSON.getBytes()), new TypeReference<List>() {}));
  }

  @Test
  public void testFromJson_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> Utils.fromJson(JSON, null));
    assertThatNullPointerException().isThrownBy(() -> Utils.fromJson((InputStream) null, TYPE));
    assertThatNullPointerException().isThrownBy(() -> Utils.fromJson((String) null, TYPE));
  }

  @Test
  public void testToUrl() throws Exception {
    final String urlString = "http://test.com:8080";
    final URL expected = new URL(urlString);
    final URL actual = Utils.toUrl(urlString);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testToUrl_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> Utils.toUrl(null));
  }

  @Test
  public void testToUrl_throwsOnMalformed() {
    final String urlStringMalformed = "http://test.com:-8080";
    assertThatExceptionOfType(AssertionError.class)
        .isThrownBy(() -> Utils.toUrl(urlStringMalformed));
  }

  @Test
  public void sanitizeOpenLineageNamespaceMatchesProjectionRules() {
    assertThat(Utils.sanitizeOpenLineageNamespace("https://example.com/a-b_c.d@e+f"))
        .isEqualTo("https://example.com/a-b_c.d@e+f");
    assertThat(Utils.sanitizeOpenLineageNamespace("namespace with spaces?#"))
        .isEqualTo("namespace_with_spaces__");
  }

  @Test
  public void openLineageRunUuidCanonicalizesValidUuidSpelling() {
    UUID expected = UUID.fromString("A0B1C2D3-E4F5-4678-9ABC-DEF012345678");

    assertThat(Utils.openLineageRunUuid("a0b1c2d3-e4f5-4678-9abc-def012345678"))
        .isEqualTo(expected);
    assertThat(Utils.openLineageRunUuid("A0B1C2D3-E4F5-4678-9ABC-DEF012345678"))
        .isEqualTo(expected);
  }

  @Test
  public void openLineageRunUuidUsesStableUtf8NameUuidForNonUuidId() {
    String runId = "rūn-事件";

    assertThat(Utils.openLineageRunUuid(runId))
        .isEqualTo(UUID.nameUUIDFromBytes(runId.getBytes(StandardCharsets.UTF_8)))
        .isEqualTo(Utils.openLineageRunUuid(runId));
  }

  @Test
  public void openLineageRunUuidRejectsNullAndBlankIds() {
    assertThatNullPointerException().isThrownBy(() -> Utils.openLineageRunUuid(null));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> Utils.openLineageRunUuid("\u2003\u00a0"))
        .withMessage("runId must not be blank");
  }

  @Test
  public void openLineageParentRunUuidUsesNormalRunIdentityForNonUuidIds() {
    String runId = "scheduled-normal-parent";
    LineageEvent.ParentRunFacet parent =
        LineageEvent.ParentRunFacet.builder()
            .run(LineageEvent.RunLink.builder().runId(runId).build())
            .job(
                LineageEvent.JobLink.builder()
                    .namespace("parent namespace")
                    .name("parent-job")
                    .build())
            .build();

    assertThat(Utils.openLineageParentRunUuid(parent, "parent-job.child"))
        .isEqualTo(Utils.openLineageRunUuid(runId));
  }

  @Test
  public void openLineageParentRunUuidIsolatesLegacyAirflowDerivation() {
    String runId = "scheduled__2022-04-25T00:20:00+00:00";
    String rawNamespace = "parent namespace";
    String taskName = "dag.task";
    LineageEvent.ParentRunFacet parent =
        LineageEvent.ParentRunFacet.builder()
            .run(LineageEvent.RunLink.builder().runId(runId).build())
            .job(LineageEvent.JobLink.builder().namespace(rawNamespace).name(taskName).build())
            .build();

    assertThat(Utils.openLineageParentRunUuid(parent, taskName))
        .isEqualTo(
            Utils.toNameBasedUuid(Utils.sanitizeOpenLineageNamespace(rawNamespace), "dag", runId));
  }

  @Test
  public void sha256Utf8HashesExactText() {
    assertThat(Utils.sha256Utf8("{\"n\":1.00}"))
        .hasSize(32)
        .isNotEqualTo(Utils.sha256Utf8("{\"n\":1.0}"));
  }

  @Test
  public void testChecksumFor_equal() {
    final Map<String, String> kvMap = ImmutableMap.of("key0", "value0", "key1", "value1");

    final String checksum0 = Utils.checksumFor(kvMap);
    final String checksum1 = Utils.checksumFor(kvMap);
    assertThat(checksum0).isEqualTo(checksum1);
  }

  @Test
  public void testChecksumFor_notEqual() {
    final Map<String, String> kvMap0 = ImmutableMap.of("key0", "value0", "key1", "value1");
    final Map<String, String> kvMap1 = ImmutableMap.of("key2", "value2", "key3", "value3");

    final String checksum0 = Utils.checksumFor(kvMap0);
    final String checksum1 = Utils.checksumFor(kvMap1);
    assertThat(checksum0).isNotEqualTo(checksum1);
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  static final class Object {
    final String value;

    @JsonCreator
    Object(final String value) {
      this.value = value;
    }
  }

  @Test
  public void testNewJobVersionFor_equal() {
    final NamespaceName namespaceName = newNamespaceName();
    final JobName jobName = newJobName();
    final JobMeta jobMeta = newJobMeta();

    // Generate version0 and version1; versions will be equal.
    final Version version0 =
        Utils.newJobVersionFor(
            namespaceName,
            jobName,
            jobMeta.getInputs(),
            jobMeta.getOutputs(),
            jobMeta.getLocation().map(URL::toString).orElse(null));
    final Version version1 =
        Utils.newJobVersionFor(
            namespaceName,
            jobName,
            jobMeta.getInputs(),
            jobMeta.getOutputs(),
            jobMeta.getLocation().map(URL::toString).orElse(null));
    assertThat(version0).isEqualTo(version1);
  }

  @Test
  public void testNewJobVersionFor_equalOnUnsortedInputsAndOutputs() {
    final NamespaceName namespaceName = newNamespaceName();
    final JobName jobName = newJobName();
    final JobMeta jobMeta = newJobMeta();

    // Generate version0 and version1; versions will be equal.
    final Version version0 =
        Utils.newJobVersionFor(
            namespaceName,
            jobName,
            jobMeta.getInputs(),
            jobMeta.getOutputs(),
            jobMeta.getLocation().map(URL::toString).orElse(null));
    // Unsort the job inputs and outputs for version1.
    final ImmutableSet<DatasetId> unsortedJobInputIds =
        jobMeta.getInputs().stream().sorted(Collections.reverseOrder()).collect(toImmutableSet());
    final ImmutableSet<DatasetId> unsortedJobOutputIds =
        jobMeta.getOutputs().stream().sorted(Collections.reverseOrder()).collect(toImmutableSet());
    final Version version1 =
        Utils.newJobVersionFor(
            namespaceName,
            jobName,
            unsortedJobInputIds,
            unsortedJobOutputIds,
            jobMeta.getLocation().map(URL::toString).orElse(null));
    assertThat(version0).isEqualTo(version1);
  }

  @Test
  public void testNewJobVersionFor_notEqual() {
    final NamespaceName namespaceName = newNamespaceName();
    final JobName jobName = newJobName();
    final JobMeta jobMeta0 = newJobMeta();
    final JobMeta jobMeta1 = newJobMeta();

    // Generate version0 and version1; versions will not be equal.
    final Version version0 =
        Utils.newJobVersionFor(
            namespaceName,
            jobName,
            jobMeta0.getInputs(),
            jobMeta0.getOutputs(),
            jobMeta0.getLocation().map(URL::toString).orElse(null));
    final Version version1 =
        Utils.newJobVersionFor(
            namespaceName,
            jobName,
            jobMeta1.getInputs(),
            jobMeta1.getOutputs(),
            jobMeta1.getLocation().map(URL::toString).orElse(null));
    assertThat(version0).isNotEqualTo(version1);
  }

  @Test
  public void testDatasetVersionForDBTableMetaDataEqualOnSameData() {
    NamespaceName namespaceName = newNamespaceName();
    DatasetName datasetName = newDatasetName();
    DbTableMeta dbTableMeta = newDbTableMeta();

    Version first =
        Utils.newDatasetVersionFor(namespaceName.getValue(), datasetName.getValue(), dbTableMeta);
    Version second =
        Utils.newDatasetVersionFor(namespaceName.getValue(), datasetName.getValue(), dbTableMeta);

    assertThat(first).isEqualTo(second);
  }

  @Test
  public void testDatasetVersionForStreamMetaDataEqualOnSameData() {
    NamespaceName namespaceName = newNamespaceName();
    DatasetName datasetName = newDatasetName();
    StreamMeta streamMeta = newStreamMeta();

    Version first =
        Utils.newDatasetVersionFor(namespaceName.getValue(), datasetName.getValue(), streamMeta);
    Version second =
        Utils.newDatasetVersionFor(namespaceName.getValue(), datasetName.getValue(), streamMeta);

    assertThat(first).isEqualTo(second);
  }

  @Test
  public void testDatasetVersionEqualOnSameData() {
    final NamespaceName namespaceName = newNamespaceName();
    DatasetName datasetName = newDatasetName();
    DatasetName physicalName = newDatasetName();
    SourceName sourceName = newSourceName();
    String lifecycleState = newLifecycleState();
    List<LineageEvent.SchemaField> schemaFields = newSchemaFields(2);
    RunId runId = newRunId();

    Version first =
        Utils.newDatasetVersionFor(
            namespaceName.getValue(),
            sourceName.getValue(),
            physicalName.getValue(),
            datasetName.getValue(),
            lifecycleState,
            schemaFields,
            runId.getValue());
    Version second =
        Utils.newDatasetVersionFor(
            namespaceName.getValue(),
            sourceName.getValue(),
            physicalName.getValue(),
            datasetName.getValue(),
            lifecycleState,
            schemaFields,
            runId.getValue());

    assertThat(first).isEqualTo(second);
  }

  @Test
  public void testDatasetVersionForDBTableMetaDataIsNotEqualOnDifferentData() {
    DbTableMeta dbTableMeta = newDbTableMeta();

    Version first =
        Utils.newDatasetVersionFor(
            newNamespaceName().getValue(), newDatasetName().getValue(), dbTableMeta);
    Version second =
        Utils.newDatasetVersionFor(
            newNamespaceName().getValue(), newNamespaceName().getValue(), dbTableMeta);

    assertThat(first).isNotEqualTo(second);
  }

  @Test
  public void testDatasetVersionForStreamMetaDataIsNotEqualOnDifferentData() {
    StreamMeta streamMeta = newStreamMeta();

    Version first =
        Utils.newDatasetVersionFor(
            newNamespaceName().getValue(), newDatasetName().getValue(), streamMeta);
    Version second =
        Utils.newDatasetVersionFor(
            newNamespaceName().getValue(), newDatasetName().getValue(), streamMeta);

    assertThat(first).isNotEqualTo(second);
  }

  @Test
  public void testDatasetVersionIsNotEqualOnDifferentData() {
    List<LineageEvent.SchemaField> schemaFields = newSchemaFields(2);

    Version first =
        Utils.newDatasetVersionFor(
            newNamespaceName().getValue(),
            newSourceName().getValue(),
            newDatasetName().getValue(),
            newDatasetName().getValue(),
            newLifecycleState(),
            schemaFields,
            newRunId().getValue());

    Version second =
        Utils.newDatasetVersionFor(
            newNamespaceName().getValue(),
            newSourceName().getValue(),
            newDatasetName().getValue(),
            newDatasetName().getValue(),
            newLifecycleState(),
            schemaFields,
            newRunId().getValue());

    assertThat(first).isNotEqualTo(second);
  }

  @Test
  public void testDatasetVersionWithNullFields() {
    Version version = Utils.newDatasetVersionFor(null, null, null, null, null, null, null);

    assertThat(version.getValue()).isNotNull();
  }

  @Test
  public void testDatasetVersionWithNullDatasetFields() {
    Version version = Utils.newDatasetVersionFor(null, null, null);

    assertThat(version.getValue()).isNotNull();
  }

  @Test
  public void testNewDatasetVersionFor_equalOnUnsortedSchemaFields() {
    final NamespaceName namespaceName = newNamespaceName();
    DatasetName datasetName = newDatasetName();
    DatasetName physicalName = newDatasetName();
    SourceName sourceName = newSourceName();
    String lifecycleState = newLifecycleState();
    List<LineageEvent.SchemaField> schemaFields = newSchemaFields(2);
    RunId runId = newRunId();

    Version first =
        Utils.newDatasetVersionFor(
            namespaceName.getValue(),
            sourceName.getValue(),
            physicalName.getValue(),
            datasetName.getValue(),
            lifecycleState,
            schemaFields,
            runId.getValue());

    List<LineageEvent.SchemaField> shuffleSchemaFields = new ArrayList<>(schemaFields);
    Collections.shuffle(shuffleSchemaFields);
    Version second =
        Utils.newDatasetVersionFor(
            namespaceName.getValue(),
            sourceName.getValue(),
            physicalName.getValue(),
            datasetName.getValue(),
            lifecycleState,
            shuffleSchemaFields,
            runId.getValue());

    assertThat(first).isEqualTo(second);
  }

  @Test
  public void testNewDatasetVersionFor_equalOnUnsortedFields() {
    NamespaceName namespaceName = newNamespaceName();
    DatasetName datasetName = newDatasetName();
    DbTableMeta dbTableMeta = newDbTableMeta();

    Version first =
        Utils.newDatasetVersionFor(namespaceName.getValue(), datasetName.getValue(), dbTableMeta);

    List<Field> fields = new ArrayList<>(dbTableMeta.getFields());
    Collections.shuffle(fields);
    DbTableMeta dbTableMetaUnsortedFields =
        new DbTableMeta(
            dbTableMeta.getPhysicalName(),
            dbTableMeta.getSourceName(),
            ImmutableList.copyOf(fields),
            dbTableMeta.getTags(),
            dbTableMeta.getDescription().orElse(null),
            dbTableMeta.getRunId().orElse(null));

    Version second =
        Utils.newDatasetVersionFor(
            namespaceName.getValue(), datasetName.getValue(), dbTableMetaUnsortedFields);

    assertThat(first).isEqualTo(second);
  }

  @Test
  void testNewDatasetSchemaVersionFor_allNulls() {
    Version version = Utils.newDatasetSchemaVersionFor(null, null, null);

    assertThat(version.getValue()).isNotNull();
  }

  @Test
  void testNewDatasetSchemaVersionFor_equalOnIdenticalInputs() {
    NamespaceName namespaceName = newNamespaceName();
    DatasetName datasetName = newDatasetName();
    List<Pair<String, String>> fields =
        List.of(
            Pair.of(newFieldName().getValue(), newFieldType()),
            Pair.of(newFieldName().getValue(), newFieldType()),
            Pair.of(newFieldName().getValue(), newFieldType()));

    Version first =
        Utils.newDatasetSchemaVersionFor(namespaceName.getValue(), datasetName.getValue(), fields);
    Version second =
        Utils.newDatasetSchemaVersionFor(namespaceName.getValue(), datasetName.getValue(), fields);

    assertThat(first).isEqualTo(second);
  }

  @Test
  void testNewDatasetSchemaVersionFor_equalOnUnsortedFields() {
    NamespaceName namespaceName = newNamespaceName();
    DatasetName datasetName = newDatasetName();
    List<Pair<String, String>> fields =
        List.of(
            Pair.of(newFieldName().getValue(), newFieldType()),
            Pair.of(newFieldName().getValue(), newFieldType()),
            Pair.of(newFieldName().getValue(), newFieldType()));

    Version first =
        Utils.newDatasetSchemaVersionFor(namespaceName.getValue(), datasetName.getValue(), fields);

    List<Pair<String, String>> shuffledFields = new ArrayList<>(fields);
    Collections.shuffle(shuffledFields);
    Version second =
        Utils.newDatasetSchemaVersionFor(
            namespaceName.getValue(), datasetName.getValue(), shuffledFields);

    assertThat(first).isEqualTo(second);
  }
}
