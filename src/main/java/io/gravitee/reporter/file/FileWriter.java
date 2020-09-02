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

import io.gravitee.reporter.api.Reportable;
import io.gravitee.reporter.file.config.FileReporterConfiguration;
import io.gravitee.reporter.file.transformer.Transformer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class FileWriter<T extends Reportable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileWriter.class);

    private final Vertx vertx;

    private final FileReporterConfiguration configuration;

    private final String type;

    private final Transformer<T> transformer;

    private AsyncFile asyncFile;

    private final SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyy_MM_dd");

    public FileWriter(Vertx vertx, FileReporterConfiguration configuration, String type, Transformer<T> transformer) {
        this.vertx = vertx;
        this.configuration = configuration;
        this.type = type;
        this.transformer = transformer;
    }

    public Future<Void> initialize() {
        Future<Void> future = Future.future();

        String fileName = configuration.getOutputDirectory() + '/' + String.format(configuration.getFilePattern(), this.type);
        int i = fileName.toLowerCase(Locale.ENGLISH).indexOf("yyyy_mm_dd");
        if (i >= 0) {
            fileName = fileName.substring(0, i) + fileDateFormat.format(new Date()) + fileName.substring(i + "yyyy_mm_dd".length());
        }

        LOGGER.info("Initializing file reporter to write into file: {}", fileName);

        vertx.fileSystem().open(fileName, new OpenOptions()
                        .setAppend(true)
                        .setDsync(true), event -> {
                            if (event.succeeded()) {
                                asyncFile = event.result();

                                vertx.setPeriodic(1000, new Handler<Long>() {
                                    @Override
                                    public void handle(Long event) {
                                        asyncFile.flush(new Handler<AsyncResult<Void>>() {
                                            @Override
                                            public void handle(AsyncResult<Void> event) {
                                                //    System.out.println("Flush");
                                            }
                                        });
                                    }
                                });

                                future.complete();
                            } else {
                                LOGGER.error("An error occurs while starting file writer for type[{}]", this.type, event.cause());
                                future.fail(event.cause());
                            }
                        }
        );

        return future;
    }

    public void write(T data) {
        asyncFile.write(transformer.transform(data));
    }

    public Future<Void> close() {
        Future<Void> future = Future.future();

        asyncFile.close(event -> {
            if (event.succeeded()) {
                asyncFile = null;
                LOGGER.info("File writer is now closed for type [{}]", this.type);
                future.complete();
            } else {
                LOGGER.error("An error occurs while closing file writer for type[{}]", this.type, event.cause());
                future.fail(event.cause());
            }
        });

        return future;
    }
}
