/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.service;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Describes a live Controller Service resolution plan computed by the
 * ControllerServiceResolver. Separates updates by component kind to avoid
 * ambiguity when applying the updates and provides unresolved identifiers.
 */
public class LiveControllerServiceResolutionPlan {
    private final Map<String, Map<String, String>> processorUpdates;
    private final Map<String, Map<String, String>> controllerServiceUpdates;
    private final Set<String> unresolved;

    public LiveControllerServiceResolutionPlan(final Map<String, Map<String, String>> processorUpdates,
                                               final Map<String, Map<String, String>> controllerServiceUpdates,
                                               final Set<String> unresolved) {
        this.processorUpdates = (processorUpdates == null) ? Collections.emptyMap() : processorUpdates;
        this.controllerServiceUpdates = (controllerServiceUpdates == null) ? Collections.emptyMap() : controllerServiceUpdates;
        this.unresolved = (unresolved == null) ? Collections.emptySet() : unresolved;
    }

    /**
     * @return Map of Processor ID -> (property name -> resolved Controller Service ID)
     */
    public Map<String, Map<String, String>> getProcessorUpdates() {
        return processorUpdates;
    }

    /**
     * @return Map of Controller Service ID -> (property name -> resolved Controller Service ID)
     */
    public Map<String, Map<String, String>> getControllerServiceUpdates() {
        return controllerServiceUpdates;
    }

    /**
     * @return Unresolved raw Controller Service identifiers encountered during planning
     */
    public Set<String> getUnresolved() {
        return unresolved;
    }
}
