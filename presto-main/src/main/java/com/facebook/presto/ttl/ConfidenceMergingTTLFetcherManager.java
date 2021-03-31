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

import com.facebook.presto.metadata.InternalNodeManager;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ConfidenceMergingTTLFetcherManager
        extends MergingTTLFetcherManager<ConfidenceBasedTTLInfo>
{
    @Inject
    public ConfidenceMergingTTLFetcherManager(Set<TTLFetcher<ConfidenceBasedTTLInfo>> ttlFetchers, InternalNodeManager nodeManager)
    {
        super(ttlFetchers, nodeManager);
    }

    @Override
    public NodeTTL<ConfidenceBasedTTLInfo> mergeTTLInfo(Collection<TTLInfo<ConfidenceBasedTTLInfo>> ttlInfo)
    {
        ImmutableSet.Builder<TTLInfo<ConfidenceBasedTTLInfo>> ttlConfidenceBuilder = ImmutableSet.builder();
        List<ConfidenceBasedTTLInfo> ttls = ttlInfo.stream().map(TTLInfo::getTTL).collect(toImmutableList());

        Collections.sort(ttls);

        for (int i = 0; i < ttls.size(); i++) {
            ConfidenceBasedTTLInfo ttl = ttls.get(i);

            if (i == 0) {
                ttlConfidenceBuilder.add(new TTLInfo<>(ttl));
                continue;
            }

            ConfidenceBasedTTLInfo prevTtl = ttls.get(i - 1);

            if (ttl.getConfidencePercentage() < prevTtl.getConfidencePercentage()) {
                ttlConfidenceBuilder.add(new TTLInfo<>(ttl));
            }
        }

        return new NodeTTL<>(ttlConfidenceBuilder.build());
    }
}
