/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.thinker0.servo;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.publish.*;
import com.netflix.servo.tag.TagList;
import com.netflix.servo.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author thinker0
 */
@Singleton
public class Servo extends AbstractModule {
    private static Logger LOG = LoggerFactory.getLogger(Servo.class);
    private static final String environment = System.getenv("SERVER_ENV") != null ? System.getenv("SERVER_ENV") : "";
    private static final String publishServerHostAndPort = System.getProperty("servo.export.host", "localhost:4242");
    private static final AtomicBoolean atomicRunning = new AtomicBoolean(false);
    private static final AsyncMetricObserver metricObserver = new AsyncMetricObserver("servo", new OpenTSDBObserver("stats"), 10, 10000);
    private static final MonitorRegistryMetricPoller poller = new MonitorRegistryMetricPoller();

    @Provides
    @Named("servo-opentsdb-publisher")
    @Singleton
    public static void useMetricListeners() {
        final int interval = Integer.parseInt(System.getProperty("servo.export.interval.sec", "60"));
        Servo.useMetricListeners(interval, TimeUnit.SECONDS);
    }

    @Provides
    @Named("servo-opentsdb-publisher-10sec")
    @Singleton
    public static void useMetricTenSecsListeners() {
        Servo.useMetricListeners(10, TimeUnit.SECONDS);
    }

    public static void useMetricListeners(long delay, TimeUnit timeUnit) {
        final PollScheduler scheduler = PollScheduler.getInstance();
        if (!atomicRunning.get()) {
            try {
                scheduler.start();
                atomicRunning.set(true);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    Servo.stopMetricListeners();
                }
            });
            LOG.info("Environment: " + environment);
            final PollRunnable task = new PollRunnable(
                    poller,
                    BasicMetricFilter.MATCH_ALL,
                    Lists.<MetricObserver>newArrayList(metricObserver));
            scheduler.addPoller(task, delay, timeUnit);
        }
    }

    @PreDestroy
    public static void stopMetricListeners() {
        final PollScheduler scheduler = PollScheduler.getInstance();
        if (scheduler.isStarted()) {
            try {
                scheduler.stop();
            } catch (Exception e) {
                //
            }
        }
    }

    @Override
    protected void configure() {
        // noop
    }

    public static class OpenTSDBObserver extends BaseMetricObserver {
        private InetSocketAddress server = null;
        private long lookupTime = 0;

        private InetSocketAddress socketAddress = null;

        /**
         * Creates a new instance with a given name.
         *
         * @param name Tag Name
         */
        public OpenTSDBObserver(String name) {
            super(name);
        }

        /**
         * nslookup 너무 자주 하면 잘 않되는 경우가 있다.
         *
         * @return InetSocketAddress
         */
        private InetSocketAddress getSocketAddress() {
            // 5minutes cache
            if (server == null || lookupTime + (60 * 1000 * 5) < System.currentTimeMillis()) {
                try {
                    final String[] hostAndPort = publishServerHostAndPort.split(":");
                    final int port = hostAndPort.length > 1 ? Integer.parseInt(hostAndPort[1]) : 4242;
                    server = new InetSocketAddress(hostAndPort[0], port);
                    lookupTime = System.currentTimeMillis();
                } catch (Exception e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
            return server;
        }

        @Override
        public void updateImpl(List<Metric> metrics) {
            Preconditions.checkNotNull(metrics, "metrics");
            if (metrics.isEmpty()) {
                // LOG.info("OpenTSDB Write: ({})", counterList.size());
                return;
            }
            // final ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
            final Charset charset = Charset.forName("UTF-8");
            final CharsetEncoder encoder = charset.newEncoder();
            try(SocketChannel client = SocketChannel.open()) {
                client.socket().setSoTimeout(3000);
                // LOG.debug("Open: '{}'", server.toString());
                client.setOption(StandardSocketOptions.SO_SNDBUF, 4096);
                client.setOption(StandardSocketOptions.SO_RCVBUF, 4000);
                client.setOption(StandardSocketOptions.TCP_NODELAY, true);
                client.setOption(StandardSocketOptions.SO_REUSEADDR, false);
                client.setOption(StandardSocketOptions.SO_LINGER, 0);
                final InetSocketAddress socketAddress = getSocketAddress();
                if (socketAddress != null) {
                    // LOG.debug("Connect: '{}'", socketAddress.toString());
                    if (client.connect(socketAddress) && client.isConnected()) {
                        for (final Metric metric : metrics) {
                            // TODO double/int/...
                            if (isCounter(metric) && metric.getNumberValue().longValue() > 0) {
                                final String tags = metricTags(metric); // metric to String
                                final String line = String.format("PUT %s %d %s %s\r\n",
                                    metric.getConfig().getName(), metric.getTimestamp(),
                                    metric.getValue().toString(), tags);
                                int write = client.write(encoder.encode(CharBuffer.wrap(line)));
                                LOG.debug("OpenTSDB Write: {} ({}) {}", line.replaceAll("\r\n", " CRLF"), line.length(), (line.length() == write));
                            }
                        }
                    }
                }
                client.shutdownOutput();
                client.shutdownInput();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        private String localhostName() {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                LOG.error(e.getMessage(), e);
            }
            return null;
        }

        /**
         * OpenTSDB Tag 제거 할 항목 Key
         */
        static final List<String> ignore_keys = Arrays.asList("host", "pid");

        private String metricTags(Metric metric) {
            final Map<String, String> map = metric.getConfig().getTags().asMap();
            final List<String> tags = Lists.newArrayList();
            final String hostName = localhostName();
            if (hostName != null) {
                tags.add(String.format("host=%s", hostName));
            }
            for (final Map.Entry<String, String> item : map.entrySet()) {
                if (ignore_keys.contains(item.getKey())) {
                    tags.add(String.format("%s=%s", item.getKey(), item.getValue()));
                }
            }
            final String[] pidHostName = ManagementFactory.getRuntimeMXBean().getName().split("@");
            if (pidHostName.length > 0) {
                tags.add("pid=" + pidHostName[0]);
            }
            if (!Strings.isNullOrEmpty(environment)) {
                tags.add("env=" + environment);
            }
            LOG.debug("tags {}", tags);
            return Joiner.on(" ").join(tags);
        }

        private boolean isCounter(Metric m) {
            final TagList tags = m.getConfig().getTags();
            final String value = tags.getValue(DataSourceType.KEY);
            if (value != null) {
                if (DataSourceType.COUNTER.name().equals(value)
                    || DataSourceType.GAUGE.name().equals(value)
                    || DataSourceType.RATE.name().equals(value)
                    || DataSourceType.NORMALIZED.name().equals(value)) {
                    return true;
                }
            }
            return false;
        }
    }
}

