/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.prodmanager.PipelineManagerModule;
import com.streamsets.pipeline.restapi.configuration.ConfigurationInjector;
import com.streamsets.pipeline.restapi.configuration.BuildInfoInjector;
import com.streamsets.pipeline.restapi.configuration.PipelineStoreInjector;
import com.streamsets.pipeline.restapi.configuration.ProductionPipelineManagerInjector;
import com.streamsets.pipeline.restapi.configuration.RestAPIResourceConfig;
import com.streamsets.pipeline.restapi.configuration.RuntimeInfoInjector;
import com.streamsets.pipeline.restapi.configuration.StageLibraryInjector;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.metrics.MetricsModule;
import com.streamsets.pipeline.restapi.RestAPI;
import com.streamsets.pipeline.stagelibrary.StageLibraryModule;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreModule;
import com.streamsets.pipeline.websockets.SDCWebSocketServlet;
import dagger.Module;
import dagger.Provides;
import dagger.Provides.Type;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;

import javax.servlet.DispatcherType;
import java.util.EnumSet;

@Module(library = true, includes = {RuntimeModule.class, MetricsModule.class,
    PipelineStoreModule.class, StageLibraryModule.class, PipelineManagerModule.class})
public class WebServerModule {

  @Provides(type = Type.SET)
  ContextConfigurator provideStaticWeb(final RuntimeInfo runtimeInfo) {
    return new ContextConfigurator() {

      @Override
      public void init(ServletContextHandler context) {
        ServletHolder servlet = new ServletHolder(new DefaultServlet());
        servlet.setInitParameter("dirAllowed", "true");
        servlet.setInitParameter("resourceBase", runtimeInfo.getStaticWebDir());
        context.addServlet(servlet, "/*");
      }

    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideGzipFilter() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        FilterHolder filter = new FilterHolder(GzipFilter.class);
        context.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST));
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideLocaleDetector() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        FilterHolder filter = new FilterHolder(new LocaleDetectorFilter());
        context.addFilter(filter, "/rest/*", EnumSet.of(DispatcherType.REQUEST));
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideJMX(final MetricRegistry metrics) {
    return new ContextConfigurator() {
      private JmxReporter reporter;
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute("com.codahale.metrics.servlets.MetricsServlet.registry", metrics);
        ServletHolder servlet = new ServletHolder(new JMXJsonServlet());
        context.addServlet(servlet, "/jmx");
      }

      @Override
      public void start() {
        reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
      }

      @Override
      public void stop() {
        reporter.stop();
        reporter.close();
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideLoginServlet() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        ServletHolder holderEvents = new ServletHolder(new LoginServlet());
        context.addServlet(holderEvents, "/login");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideWebSocketServlet(final Configuration configuration, final RuntimeInfo runtimeInfo,
                                        final PipelineManager pipelineManager) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        ServletHolder holderEvents = new ServletHolder(new SDCWebSocketServlet(configuration, runtimeInfo,
          pipelineManager));
        context.addServlet(holderEvents, "/rest/v1/webSocket");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideNoAuthenticationRoles(final Configuration configuration) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        if (configuration.get(WebServerTask.AUTHENTICATION_KEY, WebServerTask.AUTHENTICATION_DEFAULT).equals("none")) {
          FilterHolder filter = new FilterHolder(new AlwaysAllRolesFilter());
          context.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST));
        }
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideJersey() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        ServletHolder servlet = new ServletHolder(new ServletContainer());
        servlet.setInitParameter(ServerProperties.PROVIDER_PACKAGES, RestAPI.class.getPackage().getName());
        servlet.setInitParameter(ServletProperties.JAXRS_APPLICATION_CLASS, RestAPIResourceConfig.class.getName());
        context.addServlet(servlet, "/rest/*");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final PipelineStoreTask pipelineStore) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(PipelineStoreInjector.PIPELINE_STORE, pipelineStore);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final StageLibraryTask stageLibrary) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(StageLibraryInjector.STAGE_LIBRARY, stageLibrary);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final Configuration configuration) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(ConfigurationInjector.CONFIGURATION, configuration);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStateManager(final PipelineManager pipelineManager) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(ProductionPipelineManagerInjector.PIPELINE_MANAGER_MGR, pipelineManager);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideRuntimeInfo(final RuntimeInfo runtimeInfo) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(RuntimeInfoInjector.RUNTIME_INFO, runtimeInfo);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideBuildInfo(final BuildInfo buildInfo) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(BuildInfoInjector.BUILD_INFO, buildInfo);
      }
    };
  }

}