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
package com.facebook.presto.metadata;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.NodeState;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.google.common.collect.Sets.difference;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class TtlFetcherManager
{
    private static final Logger log = Logger.get(TtlFetcherManager.class);
    private final List<TtlFetcher> ttlFetchers;
    private final ScheduledExecutorService refreshTtlExecutor;
    private final InternalNodeManager nodeManager;
    private final ConcurrentHashMap<InternalNode, NodeTtl> nodeTtlMap = new ConcurrentHashMap<>();

    @Inject
    public TtlFetcherManager(List<TtlFetcher> ttlFetchers, InternalNodeManager nodeManager)
    {
        this.ttlFetchers = ttlFetchers;
        this.nodeManager = nodeManager;
        refreshTtlExecutor = newSingleThreadScheduledExecutor(threadsNamed("refresh-ttl-executor-%s"));
    }

    @PostConstruct
    public void startRefreshingTtlInfo()
    {
        refreshTtlExecutor.scheduleWithFixedDelay(() -> {
            try {
                refreshTtlInfo();
            }
            catch (Exception e) {
                log.error(e, "Error polling state of nodes");
            }
        }, 60, 900, TimeUnit.SECONDS);
        refreshTtlInfo();
    }

    private void refreshTtlInfo()
    {
        Set<InternalNode> activeNodes = nodeManager.getNodes(NodeState.ACTIVE);
        ImmutableSetMultimap.Builder<InternalNode, TtlConfidence> ttlInfoBuilder = ImmutableSetMultimap.builder();

        for (TtlFetcher ttlFetcher : ttlFetchers) {
            Map<InternalNode, NodeTtl> ttlInfo = ttlFetcher.getTtlInfo(activeNodes);
            ttlInfo.entrySet().stream().map(e -> e.getValue().getTtls().stream().map(ttl -> ttlInfoBuilder.put(e.getKey(), ttl)));
        }

        ImmutableSetMultimap<InternalNode, TtlConfidence> ttlInfo = ttlInfoBuilder.build();

        generateNodeTtls(ttlInfo).entrySet().stream().map(e -> nodeTtlMap.put(e.getKey(), e.getValue()));

        Set<InternalNode> deadNodes = difference(nodeTtlMap.keySet(), activeNodes).immutableCopy();
        nodeTtlMap.keySet().removeAll(deadNodes);
    }

    public Map<InternalNode, NodeTtl> generateNodeTtls(ImmutableSetMultimap<InternalNode, TtlConfidence> ttlInfo)
    {
        ImmutableMap.Builder<InternalNode, NodeTtl> nodeTtls = ImmutableMap.builder();

        for (Map.Entry<InternalNode, Collection<TtlConfidence>> entry : ttlInfo.asMap().entrySet()) {
            ImmutableSet.Builder<TtlConfidence> ttlConfidenceBuilder = ImmutableSet.builder();
            List<TtlConfidence> ttls = new ArrayList<>(entry.getValue());
            Collections.sort(ttls);

            for (int i = 0; i < ttls.size(); i++) {
                TtlConfidence ttl = ttls.get(i);

                if (i == 0) {
                    ttlConfidenceBuilder.add(ttl);
                    continue;
                }

                TtlConfidence prevTtl = ttls.get(i - 1);

                if (ttl.getConfidencePercentage() < prevTtl.getConfidencePercentage()) {
                    ttlConfidenceBuilder.add(ttl);
                }
            }
            nodeTtls.put(entry.getKey(), new NodeTtl(ttlConfidenceBuilder.build()));
        }
        return nodeTtls.build();
    }
}
