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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;

public class TTLFetcherConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private boolean enabled = true;
    private Set<String> fetcherTypes = ImmutableSet.of("noop");
    private String managerType = "confidence";

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("ttl-fetcher.enabled")
    public TTLFetcherConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public Set<String> getFetcherTypes()
    {
        return fetcherTypes;
    }

    @Config("ttl-fetcher.fetcher-types")
    @ConfigDescription("TTL Fetcher Manager types")
    public TTLFetcherConfig setFetcherTypes(String types)
    {
        if (types == null) {
            fetcherTypes = null;
            return this;
        }

        fetcherTypes = stream(SPLITTER.split(types)).collect(toImmutableSet());
        return this;
    }

    public String getManagerType()
    {
        return managerType;
    }

    @Config("ttl-fetcher.manager-type")
    @ConfigDescription("TTL Fetcher Manager type")
    public TTLFetcherConfig setManagerType(String type)
    {
        this.managerType = type;
        return this;
    }
}
