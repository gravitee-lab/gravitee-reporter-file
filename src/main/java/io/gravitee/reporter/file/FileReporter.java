/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.reporter.file;

import io.gravitee.common.service.AbstractService;
import io.gravitee.reporter.api.Reportable;
import io.gravitee.reporter.api.Reporter;
import io.gravitee.reporter.api.health.EndpointStatus;
import io.gravitee.reporter.api.http.Metrics;
import io.gravitee.reporter.api.log.Log;
import io.gravitee.reporter.api.monitor.Monitor;
import io.gravitee.reporter.file.config.FileReporterConfiguration;
import io.gravitee.reporter.file.transformer.EndpointStatusTransformer;
import io.gravitee.reporter.file.transformer.LogTransformer;
import io.gravitee.reporter.file.transformer.MetricsTransformer;
import io.gravitee.reporter.file.transformer.MonitorTransformer;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class FileReporter extends AbstractService implements Reporter {

    private final Logger logger = LoggerFactory.getLogger(FileReporter.class);

    @Autowired
    private Vertx vertx;

    @Autowired
    private FileReporterConfiguration configuration;

    private Map<Class<? extends Reportable>, FileWriter> writers = new HashMap<>(4);

    @Override
    public void report(Reportable reportable) {
        writers
                .get(reportable.getClass())
                .write(reportable);
    }

    @Override
    public boolean canHandle(Reportable reportable) {
        return true;
    }

    @Override
    protected void doStart() throws Exception {
        // Initialize writers
        writers.put(Metrics.class, new FileWriter<>(vertx, configuration, "request", new MetricsTransformer()));
        writers.put(Monitor.class, new FileWriter<>(vertx, configuration, "node", new MonitorTransformer()));
        writers.put(EndpointStatus.class, new FileWriter<>(vertx, configuration, "health-check", new EndpointStatusTransformer()));
        writers.put(Log.class, new FileWriter<>(vertx, configuration, "request-log", new LogTransformer()));

        CompositeFuture.join(writers.values().stream().map(FileWriter::initialize).collect(Collectors.toList()))
                .setHandler(event -> {
            if (event.succeeded()) {
                logger.info("File reporter successfully started");
            } else {
                logger.info("An error occurs while starting file reporter", event.cause());
            }
        });
    }

    @Override
    protected void doStop() throws Exception {
        CompositeFuture.join(writers.values().stream().map(FileWriter::close).collect(Collectors.toList()))
                .setHandler(event -> {
                    if (event.succeeded()) {
                        logger.info("File reporter successfully stopped");
                    } else {
                        logger.info("An error occurs while stopping file reporter", event.cause());
                    }
                });
    }
}
