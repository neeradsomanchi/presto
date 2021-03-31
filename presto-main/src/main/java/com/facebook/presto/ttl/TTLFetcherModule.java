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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class TTLFetcherModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        TTLFetcherConfig config = buildConfigObject(TTLFetcherConfig.class);
        if (!config.isEnabled()) {
            return;
        }

        String managerType = config.getManagerType();
        Set<String> ttlFetcherTypes = config.getFetcherTypes();
        if (managerType.equals("confidence")) {
            binder.bind(new TypeLiteral<TTLFetcherManager<ConfidenceBasedTTLInfo>>() {}).to(ConfidenceMergingTTLFetcherManager.class).in(Scopes.SINGLETON);
            Multibinder<TTLFetcher<ConfidenceBasedTTLInfo>> ttlFetcherBinder = newSetBinder(binder, new TypeLiteral<TTLFetcher<ConfidenceBasedTTLInfo>>() {});
            for (String ttlFetcherType : ttlFetcherTypes) {
                if (ttlFetcherType.equals("noop")) {
                    ttlFetcherBinder.addBinding().to(new TypeLiteral<NoOpTTLFetcher<ConfidenceBasedTTLInfo>>() {}).in(Scopes.SINGLETON);
                }
            }
        }
    }
}
