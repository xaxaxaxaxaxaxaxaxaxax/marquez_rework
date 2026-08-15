/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import java.util.List;
import org.jdbi.v3.core.Jdbi;

record QueueSnapshot(
    long eventCount,
    long headCount,
    long deadCount,
    long admissionCount,
    long nullAdmissionEvents,
    long minAdmissionSize,
    long maxAdmissionSize) {

  static QueueSnapshot read(Jdbi jdbi) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    WITH admission_sizes AS (
                      SELECT admission_id, count(*) AS member_count
                      FROM open_lineage_queue
                      WHERE admission_id IS NOT NULL
                      GROUP BY admission_id
                    )
                    SELECT (SELECT count(*) FROM open_lineage_queue) AS event_count,
                           (SELECT count(*) FROM open_lineage_queue_heads) AS head_count,
                           (SELECT count(*) FROM open_lineage_dead_letters) AS dead_count,
                           (SELECT count(*) FROM admission_sizes) AS admission_count,
                           (SELECT count(*) FROM open_lineage_queue
                              WHERE admission_id IS NULL) AS null_admission_events,
                           COALESCE((SELECT min(member_count) FROM admission_sizes), 0)
                               AS min_admission_size,
                           COALESCE((SELECT max(member_count) FROM admission_sizes), 0)
                               AS max_admission_size
                    """)
                .map(
                    (resultSet, context) ->
                        new QueueSnapshot(
                            resultSet.getLong("event_count"),
                            resultSet.getLong("head_count"),
                            resultSet.getLong("dead_count"),
                            resultSet.getLong("admission_count"),
                            resultSet.getLong("null_admission_events"),
                            resultSet.getLong("min_admission_size"),
                            resultSet.getLong("max_admission_size")))
                .one());
  }

  static QueueSnapshot empty() {
    return new QueueSnapshot(0, 0, 0, 0, 0, 0, 0);
  }

  static QueueSnapshot singular(long events) {
    return new QueueSnapshot(events, events, 0, 0, events, 0, 0);
  }

  static QueueSnapshot batch(long events, long admissions, long admissionSize) {
    return new QueueSnapshot(events, events, 0, admissions, 0, admissionSize, admissionSize);
  }
}

record TimedCount(int count, long nanos) {
  double perSecond() {
    return count * 1_000_000_000.0 / nanos;
  }
}

record Distribution(double median, double minimum, double maximum) {
  static Distribution of(List<Double> samples) {
    double[] ordered = samples.stream().mapToDouble(Double::doubleValue).sorted().toArray();
    if (ordered.length == 0) {
      throw new IllegalArgumentException("samples must not be empty");
    }
    int middle = ordered.length / 2;
    double median =
        (ordered.length & 1) == 1 ? ordered[middle] : (ordered[middle - 1] + ordered[middle]) / 2.0;
    return new Distribution(median, ordered[0], ordered[ordered.length - 1]);
  }
}
