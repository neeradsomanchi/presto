/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.ttl;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.NodeState;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;

import javax.inject.Inject;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.google.common.collect.Sets.difference;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public abstract class MergingTTLFetcherManager<T>
        implements TTLFetcherManager<T>
{
    private static final Logger log = Logger.get(MergingTTLFetcherManager.class);
    private final Set<TTLFetcher<T>> ttlFetchers;
    private final ScheduledExecutorService refreshTtlExecutor;
    private final InternalNodeManager nodeManager;
    private final ConcurrentHashMap<InternalNode, NodeTTL<T>> nodeTtlMap = new ConcurrentHashMap<>();

    @Inject
    public MergingTTLFetcherManager(Set<TTLFetcher<T>> ttlFetchers, InternalNodeManager nodeManager)
    {
        this.ttlFetchers = ttlFetchers;
        this.nodeManager = nodeManager;
        refreshTtlExecutor = newSingleThreadScheduledExecutor(threadsNamed("refresh-ttl-executor-%s"));
    }

    public void start()
    {
        refreshTtlExecutor.scheduleWithFixedDelay(() -> {
            try {
                refreshTtlInfo();
            }
            catch (Exception e) {
                log.error(e, "Error fetching TTLs!");
            }
//         TODO: Make configurable
        }, 60, 60, TimeUnit.SECONDS);
        refreshTtlInfo();
    }

    public void stop()
    {
        refreshTtlExecutor.shutdownNow();
    }

    private void refreshTtlInfo()
    {
        Set<InternalNode> activeNodes = nodeManager.getNodes(NodeState.ACTIVE);
        ImmutableSetMultimap.Builder<InternalNode, TTLInfo<T>> ttlInfoBuilder = ImmutableSetMultimap.builder();

        log.info("TTL FETCHERS: %s", ttlFetchers);
        for (TTLFetcher<T> ttlFetcher : ttlFetchers) {
            Map<InternalNode, NodeTTL<T>> ttlInfo = ttlFetcher.getTTLInfo(activeNodes);
            log.info("TTL Info from fetcher: %s", ttlInfo);
            ttlInfo.forEach((key, value) -> ttlInfoBuilder.putAll(key, value.getTTLInfo()));
        }

        ImmutableSetMultimap<InternalNode, TTLInfo<T>> ttlInfo = ttlInfoBuilder.build();

        log.info("TTL SetMultiMap: %s", ttlInfo);

        Map<InternalNode, NodeTTL<T>> finalNodeTtlMap = generateNodeTTLs(ttlInfo);

        log.info("FinalNodeTtlMap: %s", finalNodeTtlMap);

        nodeTtlMap.putAll(finalNodeTtlMap);

        log.info("Pre-filter nodeTtlMap: %s", nodeTtlMap);

        Set<InternalNode> deadNodes = difference(nodeTtlMap.keySet(), activeNodes).immutableCopy();
        nodeTtlMap.keySet().removeAll(deadNodes);

        log.info("TTLs refreshed, nodeTtlMap: %s", nodeTtlMap);
    }

    public abstract NodeTTL<T> mergeTTLInfo(Collection<TTLInfo<T>> ttlInfo);

    private Map<InternalNode, NodeTTL<T>> generateNodeTTLs(ImmutableSetMultimap<InternalNode, TTLInfo<T>> ttlInfo)
    {
        ImmutableMap.Builder<InternalNode, NodeTTL<T>> nodeTtls = ImmutableMap.builder();

        for (Map.Entry<InternalNode, Collection<TTLInfo<T>>> entry : ttlInfo.asMap().entrySet()) {
            nodeTtls.put(entry.getKey(), mergeTTLInfo(entry.getValue()));
        }
        return nodeTtls.build();
    }

    public Optional<NodeTTL<T>> getTTLInfo(InternalNode node)
    {
        return nodeTtlMap.containsKey(node) ? Optional.of(nodeTtlMap.get(node)) : Optional.empty();
    }
}
