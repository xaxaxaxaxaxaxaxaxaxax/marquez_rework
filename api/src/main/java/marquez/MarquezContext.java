/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import graphql.kickstart.servlet.GraphQLHttpServlet;
import io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import marquez.api.ColumnLineageResource;
import marquez.api.DatasetResource;
import marquez.api.JobResource;
import marquez.api.NamespaceResource;
import marquez.api.OpenLineageResource;
import marquez.api.SearchResource;
import marquez.api.SourceResource;
import marquez.api.StatsResource;
import marquez.api.TagResource;
import marquez.api.exceptions.JdbiExceptionExceptionMapper;
import marquez.db.BaseDao;
import marquez.db.ColumnLineageDao;
import marquez.db.DatasetDao;
import marquez.db.DatasetFieldDao;
import marquez.db.DatasetVersionDao;
import marquez.db.JobDao;
import marquez.db.JobFacetsDao;
import marquez.db.JobVersionDao;
import marquez.db.LineageDao;
import marquez.db.NamespaceDao;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageEventDao;
import marquez.db.OpenLineageProjector;
import marquez.db.OpenLineageQueueDao;
import marquez.db.RunArgsDao;
import marquez.db.RunDao;
import marquez.db.RunFacetsDao;
import marquez.db.RunStateDao;
import marquez.db.SearchDao;
import marquez.db.SourceDao;
import marquez.db.StatsDao;
import marquez.db.TagDao;
import marquez.graphql.GraphqlSchemaBuilder;
import marquez.graphql.MarquezGraphqlServletBuilder;
import marquez.search.SearchConfig;
import marquez.service.ColumnLineageService;
import marquez.service.DatasetFieldService;
import marquez.service.DatasetService;
import marquez.service.DatasetVersionService;
import marquez.service.JobService;
import marquez.service.LineageService;
import marquez.service.NamespaceService;
import marquez.service.OpenLineageConfig;
import marquez.service.OpenLineageIntake;
import marquez.service.OpenLineageService;
import marquez.service.OpenLineageWorker;
import marquez.service.RunService;
import marquez.service.RunTransitionListener;
import marquez.service.SearchService;
import marquez.service.ServiceFactory;
import marquez.service.SourceService;
import marquez.service.StatsService;
import marquez.service.TagService;
import marquez.service.models.Tag;
import org.jdbi.v3.core.Jdbi;

@Getter
public final class MarquezContext {
  private final NamespaceDao namespaceDao;
  private final SourceDao sourceDao;
  private final DatasetDao datasetDao;
  private final DatasetFieldDao datasetFieldDao;
  private final DatasetVersionDao datasetVersionDao;
  private final JobDao jobDao;
  private final JobVersionDao jobVersionDao;
  private final JobFacetsDao jobFacetsDao;
  private final RunDao runDao;
  private final RunArgsDao runArgsDao;
  private final RunFacetsDao runFacetsDao;
  private final RunStateDao runStateDao;
  private final TagDao tagDao;
  private final OpenLineageDao openLineageDao;
  private final OpenLineageEventDao openLineageEventDao;
  private final OpenLineageQueueDao openLineageQueueDao;
  private final LineageDao lineageDao;
  private final ColumnLineageDao columnLineageDao;
  private final SearchDao searchDao;
  private final StatsDao statsDao;
  private final List<RunTransitionListener> runTransitionListeners;

  private final NamespaceService namespaceService;
  private final SourceService sourceService;
  private final DatasetService datasetService;
  private final JobService jobService;
  private final TagService tagService;
  private final RunService runService;
  private final OpenLineageService openLineageService;
  private final OpenLineageIntake openLineageIntake;
  private final OpenLineageWorker openLineageWorker;
  private final LineageService lineageService;
  private final ColumnLineageService columnLineageService;
  private final SearchService searchService;
  private final StatsService statsService;
  private final NamespaceResource namespaceResource;
  private final SourceResource sourceResource;
  private final DatasetResource datasetResource;
  private final ColumnLineageResource columnLineageResource;
  private final JobResource jobResource;
  private final TagResource tagResource;
  private final OpenLineageResource openLineageResource;
  private final marquez.api.v2beta.SearchResource v2BetasearchResource;
  private final SearchResource searchResource;
  private final StatsResource opsResource;
  private final ImmutableList<Object> resources;
  private final JdbiExceptionExceptionMapper jdbiException;
  private final JsonProcessingExceptionMapper jsonException;
  private final GraphQLHttpServlet graphqlServlet;
  private final SearchConfig searchConfig;

  private MarquezContext(
      @NonNull final Jdbi jdbi,
      @NonNull final SearchConfig searchConfig,
      @NonNull final OpenLineageConfig openLineageConfig,
      @NonNull final MetricRegistry metricRegistry,
      @NonNull final ImmutableSet<Tag> tags,
      List<RunTransitionListener> runTransitionListeners) {
    List<RunTransitionListener> listeners =
        runTransitionListeners == null ? new ArrayList<>() : runTransitionListeners;
    this.searchConfig = searchConfig;

    final BaseDao baseDao = jdbi.onDemand(NamespaceDao.class);
    this.namespaceDao = jdbi.onDemand(NamespaceDao.class);
    this.sourceDao = jdbi.onDemand(SourceDao.class);
    this.datasetDao = jdbi.onDemand(DatasetDao.class);
    this.datasetFieldDao = jdbi.onDemand(DatasetFieldDao.class);
    this.datasetVersionDao = jdbi.onDemand(DatasetVersionDao.class);
    this.jobDao = jdbi.onDemand(JobDao.class);
    this.jobVersionDao = jdbi.onDemand(JobVersionDao.class);
    this.jobFacetsDao = jdbi.onDemand(JobFacetsDao.class);
    this.runDao = jdbi.onDemand(RunDao.class);
    this.runArgsDao = jdbi.onDemand(RunArgsDao.class);
    this.runFacetsDao = jdbi.onDemand(RunFacetsDao.class);
    this.runStateDao = jdbi.onDemand(RunStateDao.class);
    this.tagDao = jdbi.onDemand(TagDao.class);
    this.openLineageDao = jdbi.onDemand(OpenLineageDao.class);
    this.openLineageEventDao = jdbi.onDemand(OpenLineageEventDao.class);
    this.openLineageQueueDao = jdbi.onDemand(OpenLineageQueueDao.class);
    this.lineageDao = jdbi.onDemand(LineageDao.class);
    this.columnLineageDao = jdbi.onDemand(ColumnLineageDao.class);
    this.searchDao = jdbi.onDemand(SearchDao.class);
    this.statsDao = jdbi.onDemand(StatsDao.class);
    this.runTransitionListeners = listeners;

    this.namespaceService = new NamespaceService(baseDao);
    this.sourceService = new SourceService(baseDao);
    this.runService = new RunService(baseDao, listeners);
    this.datasetService = new DatasetService(datasetDao, runService);

    this.jobService = new JobService(baseDao, runService);
    this.tagService = new TagService(baseDao);
    this.tagService.init(tags);
    this.searchService = new SearchService(searchConfig);
    this.openLineageService =
        new OpenLineageService(
            openLineageDao,
            openLineageEventDao,
            OpenLineageProjector.getInstance(),
            runService,
            searchService);
    this.openLineageWorker =
        new OpenLineageWorker(jdbi, openLineageService, openLineageConfig, metricRegistry);
    this.openLineageIntake = new OpenLineageIntake(openLineageQueueDao, openLineageWorker::wakeUp);
    this.lineageService = new LineageService(lineageDao, jobDao, runDao);
    this.columnLineageService = new ColumnLineageService(columnLineageDao, datasetFieldDao);
    this.statsService = new StatsService(statsDao);
    this.jdbiException = new JdbiExceptionExceptionMapper();
    this.jsonException = new JsonProcessingExceptionMapper(false);
    final ServiceFactory serviceFactory =
        ServiceFactory.builder()
            .datasetService(datasetService)
            .jobService(jobService)
            .runService(runService)
            .namespaceService(namespaceService)
            .tagService(tagService)
            .openLineageService(openLineageService)
            .searchService(searchService)
            .sourceService(sourceService)
            .lineageService(lineageService)
            .columnLineageService(columnLineageService)
            .datasetFieldService(new DatasetFieldService(baseDao))
            .datasetVersionService(new DatasetVersionService(baseDao))
            .statsService(statsService)
            .build();
    this.namespaceResource = new NamespaceResource(serviceFactory);
    this.sourceResource = new SourceResource(serviceFactory);
    this.datasetResource = new DatasetResource(serviceFactory);
    this.columnLineageResource = new ColumnLineageResource(serviceFactory);
    this.jobResource = new JobResource(serviceFactory, jobVersionDao, jobFacetsDao, runFacetsDao);
    this.tagResource = new TagResource(serviceFactory);
    this.openLineageResource =
        new OpenLineageResource(serviceFactory, openLineageEventDao, openLineageIntake);
    this.searchResource = new SearchResource(searchDao);
    this.opsResource = new StatsResource(serviceFactory);
    this.v2BetasearchResource = new marquez.api.v2beta.SearchResource(serviceFactory);

    this.resources =
        ImmutableList.of(
            namespaceResource,
            sourceResource,
            datasetResource,
            columnLineageResource,
            jobResource,
            tagResource,
            jdbiException,
            jsonException,
            openLineageResource,
            searchResource,
            v2BetasearchResource,
            opsResource);

    final MarquezGraphqlServletBuilder servlet = new MarquezGraphqlServletBuilder();
    this.graphqlServlet = servlet.getServlet(new GraphqlSchemaBuilder(jdbi));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Jdbi jdbi;
    private SearchConfig searchConfig;
    private OpenLineageConfig openLineageConfig = new OpenLineageConfig();
    private MetricRegistry metricRegistry = new MetricRegistry();
    private ImmutableSet<Tag> tags = ImmutableSet.of();
    private final List<RunTransitionListener> runTransitionListeners = new ArrayList<>();

    Builder() {}

    public Builder jdbi(@NonNull Jdbi jdbi) {
      this.jdbi = jdbi;
      return this;
    }

    public Builder searchConfig(@NonNull SearchConfig searchConfig) {
      this.searchConfig = searchConfig;
      return this;
    }

    public Builder openLineageConfig(@NonNull OpenLineageConfig openLineageConfig) {
      this.openLineageConfig = openLineageConfig;
      return this;
    }

    public Builder metricRegistry(@NonNull MetricRegistry metricRegistry) {
      this.metricRegistry = metricRegistry;
      return this;
    }

    public Builder tags(@NonNull ImmutableSet<Tag> tags) {
      this.tags = tags;
      return this;
    }

    public Builder runTransitionListener(@NonNull RunTransitionListener runTransitionListener) {
      return runTransitionListeners(List.of(runTransitionListener));
    }

    public Builder runTransitionListeners(
        @NonNull List<RunTransitionListener> runTransitionListeners) {
      this.runTransitionListeners.addAll(runTransitionListeners);
      return this;
    }

    public MarquezContext build() {
      return new MarquezContext(
          jdbi, searchConfig, openLineageConfig, metricRegistry, tags, runTransitionListeners);
    }
  }
}
