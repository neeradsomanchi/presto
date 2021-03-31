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

import java.time.Instant;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ConfidenceBasedTTLInfo
        implements Comparable<ConfidenceBasedTTLInfo>
{
    private final Instant expiryEpochTime;
    private final Double confidencePercentage;

    public ConfidenceBasedTTLInfo(long expiryEpochTime, double confidencePercentage)
    {
        this.expiryEpochTime = Instant.ofEpochSecond(expiryEpochTime);
        this.confidencePercentage = confidencePercentage;
    }

    public Instant getExpiryEpochTime()
    {
        return expiryEpochTime;
    }

    public Double getConfidencePercentage()
    {
        return confidencePercentage;
    }

    @Override
    public int compareTo(ConfidenceBasedTTLInfo o)
    {
        int compareExpiryTime = expiryEpochTime.compareTo(o.getExpiryEpochTime());

        if (compareExpiryTime != 0) {
            return compareExpiryTime;
        }

        return confidencePercentage.compareTo(o.getConfidencePercentage());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expiryEpochTime, confidencePercentage);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ConfidenceBasedTTLInfo other = (ConfidenceBasedTTLInfo) obj;
        return expiryEpochTime.equals(other.getExpiryEpochTime()) &&
                confidencePercentage.equals(other.getConfidencePercentage());
    }

    public String toString()
    {
        return toStringHelper(this)
                .add("expiryEpochTime", expiryEpochTime)
                .add("confidencePercentage", confidencePercentage)
                .toString();
    }
}
