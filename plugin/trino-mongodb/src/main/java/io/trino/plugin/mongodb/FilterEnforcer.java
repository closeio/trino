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
package io.trino.plugin.mongodb;

import io.trino.spi.TrinoException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;

public class FilterEnforcer
{
    private Map<String, List<String>> requiredFilters;

    public FilterEnforcer(String requiredFiltersConfig)
    {
        this.requiredFilters = new HashMap<>();
        if (requiredFiltersConfig == null) {
            return;
        }
        for (String entry : requiredFiltersConfig.split(",")) {
            String[] tokens = entry.split(":");
            List<String> list = this.requiredFilters.computeIfAbsent(tokens[0], k -> new ArrayList<>());
            list.add(tokens[1]);
        }
    }

    public void checkAndRaiseIfInvalid(String collectionName, Document filter)
            throws TrinoException
    {
        List<String> requiredFilters = this.requiredFilters.get(collectionName);
        if (requiredFilters == null) {
            return;
        }
        for (String requiredFilter : requiredFilters) {
            if (requiredFilter != null && !contains(filter, requiredFilter)) {
                throw new TrinoException(QUERY_REJECTED, "Collection '%s' requires a filter on '%s'!".formatted(collectionName, requiredFilter));
            }
        }
    }

    private static boolean contains(Object object, String requiredFilter)
    {
        if (object instanceof Document documentEntry) {
            if (documentEntry.containsKey(requiredFilter)) {
                return true;
            }
            else {
                for (Object item : documentEntry.values()) {
                    if (contains(item, requiredFilter)) {
                        return true;
                    }
                }
            }
        }
        else if (object instanceof Iterable iterableEntry) {
            for (Object item : iterableEntry) {
                if (contains(item, requiredFilter)) {
                    return true;
                }
            }
        }
        return false;
    }
}
