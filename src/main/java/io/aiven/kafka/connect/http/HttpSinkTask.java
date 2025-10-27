/*
 * Copyright 2021 Aiven Oy and http-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.http;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import io.aiven.kafka.connect.http.recordsender.RecordSender;
import io.aiven.kafka.connect.http.sender.HttpSenderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

    private RecordSender recordSender;
    private ErrantRecordReporter reporter;

    // flag for legacy support (configured for batch mode, not using errors.tolerance)
    private boolean useLegacySend;

    // buffering for batch sending
    private final List<SinkRecord> buffer = new ArrayList<>();
    private int batchMaxSize;
    private long batchMaxTimeMs;
    private boolean batchBufferingEnabled;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFlush;

    // required by Connect
    public HttpSinkTask() {
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props);
        final var config = new HttpSinkConfig(props);
        final var httpSender = HttpSenderFactory.createHttpSender(config);
        this.recordSender = RecordSender.createRecordSender(httpSender, config);
        this.useLegacySend = config.batchingEnabled();
        this.batchBufferingEnabled = config.batchBufferingEnabled();

        if (Objects.nonNull(config.kafkaRetryBackoffMs())) {
            context.timeout(config.kafkaRetryBackoffMs());
        }

        try {
            if (context.errantRecordReporter() == null) {
                log.info("Errant record reporter not configured.");
            }

            // may be null if DLQ not enabled
            this.reporter = context.errantRecordReporter();
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            // Will occur in Connect runtimes earlier than 2.6
            log.warn("Apache Kafka versions prior to 2.6 do not support the errant record reporter.");
        }

        this.batchMaxSize = config.batchMaxSize(); // get from config
        this.batchMaxTimeMs = config.batchMaxTimeMs(); // get from config

        // Only start scheduler if both batching and buffering are enabled
        if (useLegacySend && batchBufferingEnabled) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduledFlush = scheduler.scheduleAtFixedRate(
                    this::flushBufferSafely,
                    batchMaxTimeMs,
                    batchMaxTimeMs,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        log.debug("Received {} records", records.size());
        if (useLegacySend) {
            sendBatch(records);
        } else {
            if (!records.isEmpty()) {
                sendEach(records);
            }
        }
    }

    private void flushBuffer() {
        final List<SinkRecord> toSend;
        synchronized (buffer) {
            if (buffer.isEmpty()) {
                return;
            }
            toSend = new ArrayList<>(buffer);
            buffer.clear();
        }
        for (final var record : toSend) {
            if (record.value() == null) {
                throw new DataException("Record value must not be null");
            }
        }
        try {
            recordSender.send(toSend);
        } catch (final ConnectException e) {
            if (reporter != null) {
                for (final var record : toSend) {
                    reporter.report(record, e);
                }
            } else {
                throw new ConnectException(e.getMessage());
            }
        }
    }


    private void flushBufferSafely() {
        try {
            flushBuffer();
        } catch (final Exception e) {
            log.error("Error during scheduled buffer flush", e);
        }
    }

    /**
     * Send a request per record
     * @param records to send a request per record
     */
    private void sendEach(final Collection<SinkRecord> records) {
        // send records to the sender one at a time
        for (final var record : records) {
            if (record.value() == null) {
                // TODO: consider optionally process them, e.g. use another verb or ignore
                throw new DataException("Record value must not be null");
            }

            try {
                recordSender.send(record);
            } catch (final ConnectException e) {
                if (reporter != null) {
                    reporter.report(record, e);
                } else {
                    // otherwise, re-throw the exception
                    throw new ConnectException(e.getMessage());
                }
            }
        }
    }

    /**
     * Send a single request with all records included
     * @param records to send
     */
    private void sendBatch(final Collection<SinkRecord> records) {
        synchronized (buffer) {
            // if buffering is disabled, add all records to buffer and flush immediately
            if (!batchBufferingEnabled) {
                buffer.addAll(records);
                flushBuffer();
            } else {
                for (final SinkRecord record : records) {
                    buffer.add(record);
                    if (buffer.size() >= batchMaxSize) {
                        flushBuffer();
                    }
                }
            }
        }
    }


    @Override
    public void stop() {
        if (scheduledFlush != null) {
            scheduledFlush.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        flushBuffer();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

}
