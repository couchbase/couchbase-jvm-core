/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.env;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Provides access to various metrics helpful for system diagnosis.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class Diagnostics {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Diagnostics.class);

    public static final OperatingSystemMXBean OS_BEAN = ManagementFactory.getOperatingSystemMXBean();
    public static final MemoryMXBean MEM_BEAN = ManagementFactory.getMemoryMXBean();
    public static final RuntimeMXBean RUNTIME_BEAN = ManagementFactory.getRuntimeMXBean();
    public static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

    /**
     * Collects system information as delivered from the {@link OperatingSystemMXBean}.
     *
     * @param infos a map where the infos are passed in.
     */
    @IgnoreJRERequirement //this method safely checks that the com.sun bean is available before attempting to use its methods
    public static void systemInfo(final Map<String, Object> infos) {
        infos.put("sys.os.name", OS_BEAN.getName());
        infos.put("sys.os.version", OS_BEAN.getVersion());
        infos.put("sys.os.arch", OS_BEAN.getArch());
        infos.put("sys.cpu.num", OS_BEAN.getAvailableProcessors());
        infos.put("sys.cpu.loadAvg", OS_BEAN.getSystemLoadAverage());

        try {
            if (OS_BEAN instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunBean = (com.sun.management.OperatingSystemMXBean) OS_BEAN;

                infos.put("proc.cpu.time", sunBean.getProcessCpuTime());
                infos.put("mem.physical.total", sunBean.getTotalPhysicalMemorySize());
                infos.put("mem.physical.free", sunBean.getFreePhysicalMemorySize());
                infos.put("mem.virtual.comitted", sunBean.getCommittedVirtualMemorySize());
                infos.put("mem.swap.total", sunBean.getTotalSwapSpaceSize());
                infos.put("mem.swap.free", sunBean.getFreeSwapSpaceSize());
            }
        } catch (final Throwable err) {
            LOGGER.debug("com.sun.management.OperatingSystemMXBean not available, skipping extended system info!");
        }
    }

    /**
     * Collects system information as delivered from the {@link GarbageCollectorMXBean}.
     *
     * @param infos a map where the infos are passed in.
     */
    public static void gcInfo(final Map<String, Object> infos) {
        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();

        for (GarbageCollectorMXBean mxBean : mxBeans) {
            infos.put("gc." + mxBean.getName().toLowerCase() + ".collectionCount", mxBean.getCollectionCount());
            infos.put("gc." + mxBean.getName().toLowerCase() + ".collectionTime", mxBean.getCollectionTime());
        }
    }

    /**
     * Collects system information as delivered from the {@link MemoryMXBean}.
     *
     * @param infos a map where the infos are passed in.
     */
    public static void memInfo(final Map<String, Object> infos) {
        infos.put("heap.used", MEM_BEAN.getHeapMemoryUsage());
        infos.put("offHeap.used", MEM_BEAN.getNonHeapMemoryUsage());
        infos.put("heap.pendingFinalize", MEM_BEAN.getObjectPendingFinalizationCount());
    }

    /**
     * Collects system information as delivered from the {@link RuntimeMXBean}.
     *
     * @param infos a map where the infos are passed in.
     */
    public static void runtimeInfo(final Map<String, Object> infos) {
        infos.put("runtime.vm", RUNTIME_BEAN.getVmVendor() + "/" + RUNTIME_BEAN.getVmName() + ": "
            + RUNTIME_BEAN.getVmVersion());
        infos.put("runtime.startTime", RUNTIME_BEAN.getStartTime());
        infos.put("runtime.uptime", RUNTIME_BEAN.getUptime());
        infos.put("runtime.name", RUNTIME_BEAN.getName());
        infos.put("runtime.spec", RUNTIME_BEAN.getSpecVendor() + "/" + RUNTIME_BEAN.getSpecName() + ": "
            + RUNTIME_BEAN.getSpecVersion());
        infos.put("runtime.sysProperties", RUNTIME_BEAN.getSystemProperties());
    }

    /**
     * Collects system information as delivered from the {@link ThreadMXBean}.
     *
     * @param infos a map where the infos are passed in.
     */
    public static void threadInfo(final Map<String, Object> infos) {
        infos.put("thread.count", THREAD_BEAN.getThreadCount());
        infos.put("thread.peakCount", THREAD_BEAN.getPeakThreadCount());
        infos.put("thread.startedCount", THREAD_BEAN.getTotalStartedThreadCount());
    }

    /**
     * Collects all available infos in one map.
     *
     * @return the map populated with the information.
     */
    public static Map<String, Object> collect() {
        Map<String, Object> infos = new TreeMap<String, Object>();

        systemInfo(infos);
        memInfo(infos);
        threadInfo(infos);
        gcInfo(infos);
        runtimeInfo(infos);

        return infos;
    }

    /**
     * Collects all available infos and formats it in a better readable way.
     *
     * @return a formatted string of available information.
     */
    public static String collectAndFormat() {
        Map<String, Object> infos = collect();

        StringBuilder sb = new StringBuilder();

        sb.append("Diagnostics {\n");
        int count = 0;
        for (Map.Entry<String, Object> info : infos.entrySet()) {
            if (count++ > 0) {
                sb.append(",\n");
            }
            sb.append("  ").append(info.getKey()).append("=").append(info.getValue());
        }
        sb.append("\n}");
        return sb.toString();
    }
}
