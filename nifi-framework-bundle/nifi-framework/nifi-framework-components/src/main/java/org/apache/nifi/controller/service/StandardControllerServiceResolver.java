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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class StandardControllerServiceResolver implements ControllerServiceResolver {

    private final Authorizer authorizer;
    private final FlowManager flowManager;
    private final NiFiRegistryFlowMapper flowMapper;
    private final ControllerServiceProvider controllerServiceProvider;
    private final ControllerServiceApiLookup controllerServiceApiLookup;

    public StandardControllerServiceResolver(final Authorizer authorizer,
                                             final FlowManager flowManager,
                                             final NiFiRegistryFlowMapper flowMapper,
                                             final ControllerServiceProvider controllerServiceProvider,
                                             final ControllerServiceApiLookup controllerServiceApiLookup) {
        this.authorizer = authorizer;
        this.flowManager = flowManager;
        this.flowMapper = flowMapper;
        this.controllerServiceProvider = controllerServiceProvider;
        this.controllerServiceApiLookup = controllerServiceApiLookup;
    }

    @Override
    public Set<String> resolveInheritedControllerServices(final FlowSnapshotContainer flowSnapshotContainer, final String parentGroupId, final NiFiUser user) {
        final RegisteredFlowSnapshot topLevelSnapshot = flowSnapshotContainer.getFlowSnapshot();
        final VersionedProcessGroup versionedGroup = topLevelSnapshot.getFlowContents();
        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences = topLevelSnapshot.getExternalControllerServices();

        final ProcessGroup parentGroup = flowManager.getGroup(parentGroupId);

        final Set<VersionedControllerService> ancestorServices = parentGroup.getControllerServices(true).stream()
                .filter(serviceNode -> serviceNode.isAuthorized(authorizer, RequestAction.READ, user))
                .map(serviceNode -> flowMapper.mapControllerService(serviceNode, controllerServiceProvider, new HashSet<>(), new HashMap<>()))
                .collect(Collectors.toSet());

        final Stack<Set<VersionedControllerService>> serviceHierarchyStack = new Stack<>(); //NOPMD
        serviceHierarchyStack.push(ancestorServices);

        final Set<String> unresolvedServices = new HashSet<>();
        resolveInheritedControllerServices(flowSnapshotContainer, versionedGroup, externalControllerServiceReferences, serviceHierarchyStack, unresolvedServices);
        return unresolvedServices;
    }

    /**
     * Resolves inherited controller services after property migration has occurred. This method maps the live
     * components back to a versioned snapshot, runs the standard resolution logic on that snapshot, and then
     * applies the resolved controller service references back to the live components.
     *
     * @param group the process group containing the components to resolve
     * @param externalControllerServiceReferences the external controller service references from the original flow snapshot
     * @param user the user performing the operation
     * @return the set of unresolved controller service identifiers
     */
    @Override
    public Set<String> resolveInheritedControllerServicesPostMigration(final ProcessGroup group,
                                                                        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                                        final NiFiUser user) {
        if (group == null || externalControllerServiceReferences == null || externalControllerServiceReferences.isEmpty()) {
            return Collections.emptySet();
        }

        // Step 1: Map the live process group back to a versioned snapshot
        final InstantiatedVersionedProcessGroup versionedGroup = flowMapper.mapNonVersionedProcessGroup(group, controllerServiceProvider);

        // Step 2: Create a flow snapshot container with the external controller service references
        final RegisteredFlowSnapshot snapshot = new RegisteredFlowSnapshot();
        snapshot.setFlowContents(versionedGroup);
        snapshot.setExternalControllerServices(externalControllerServiceReferences);
        final FlowSnapshotContainer container = new FlowSnapshotContainer(snapshot);

        // Step 3: Run the existing snapshot-based resolution logic
        final String parentGroupId = group.getParent() != null ? group.getParent().getIdentifier() : group.getIdentifier();
        final Set<String> unresolvedServices = resolveInheritedControllerServices(container, parentGroupId, user);

        // Step 4: Apply the resolved controller service references back to the live components
        applyResolvedReferences(group, versionedGroup);

        return unresolvedServices;
    }

    /**
     * Applies resolved controller service references from a versioned snapshot back to live components.
     */
    private void applyResolvedReferences(final ProcessGroup liveGroup, final VersionedProcessGroup resolvedSnapshot) {
        // Build a map of versioned IDs to instance IDs for all available services in the group and ancestors
        final Map<String, String> versionedIdToInstanceId = buildVersionedToInstanceIdMap(liveGroup);

        // Apply processor property updates
        if (resolvedSnapshot.getProcessors() != null) {
            for (final VersionedProcessor versionedProcessor : resolvedSnapshot.getProcessors()) {
                final String instanceId = versionedProcessor.getInstanceIdentifier();
                if (instanceId != null) {
                    final ProcessorNode liveProcessor = liveGroup.getProcessor(instanceId);
                    if (liveProcessor != null && versionedProcessor.getProperties() != null) {
                        applyPropertiesToComponent(liveProcessor, versionedProcessor.getProperties(), versionedIdToInstanceId);
                    }
                }
            }
        }

        // Apply controller service property updates
        if (resolvedSnapshot.getControllerServices() != null) {
            for (final VersionedControllerService versionedService : resolvedSnapshot.getControllerServices()) {
                final String instanceId = versionedService.getInstanceIdentifier();
                if (instanceId != null) {
                    final ControllerServiceNode liveService = liveGroup.getControllerService(instanceId);
                    if (liveService != null && versionedService.getProperties() != null) {
                        applyPropertiesToComponent(liveService, versionedService.getProperties(), versionedIdToInstanceId);
                    }
                }
            }
        }

        // Recursively apply to child process groups
        if (resolvedSnapshot.getProcessGroups() != null) {
            for (final VersionedProcessGroup childSnapshot : resolvedSnapshot.getProcessGroups()) {
                final String instanceId = childSnapshot.getInstanceIdentifier();
                if (instanceId != null) {
                    final ProcessGroup childGroup = liveGroup.getProcessGroup(instanceId);
                    if (childGroup != null) {
                        applyResolvedReferences(childGroup, childSnapshot);
                    }
                }
            }
        }
    }

    /**
     * Builds a map from versioned component IDs to instance IDs for all controller services
     * available to the given group (including ancestor services).
     */
    private Map<String, String> buildVersionedToInstanceIdMap(final ProcessGroup group) {
        final Map<String, String> versionedToInstanceMap = new HashMap<>();

        // Add services from current group and all ancestors
        ProcessGroup currentGroup = group;
        while (currentGroup != null) {
            for (final ControllerServiceNode service : currentGroup.getControllerServices(false)) {
                final String versionedId = service.getVersionedComponentId().orElse(null);
                if (versionedId != null) {
                    versionedToInstanceMap.put(versionedId, service.getIdentifier());
                }
            }
            currentGroup = currentGroup.getParent();
        }

        return versionedToInstanceMap;
    }

    /**
     * Applies controller service property references from a versioned snapshot to a live component.
     * Only updates properties that reference controller services and have changed values.
     * Converts versioned IDs to instance IDs using the provided map.
     */
    private void applyPropertiesToComponent(final ComponentNode component, final Map<String, String> snapshotProperties,
                                           final Map<String, String> versionedIdToInstanceId) {
        if (snapshotProperties == null || snapshotProperties.isEmpty()) {
            return;
        }

        final Map<String, String> updates = new HashMap<>();

        for (final Map.Entry<String, String> entry : snapshotProperties.entrySet()) {
            final String propertyName = entry.getKey();
            final String snapshotValue = entry.getValue();

            final PropertyDescriptor descriptor = component.getPropertyDescriptor(propertyName);
            if (descriptor == null) {
                continue;
            }

            // Only update properties that reference controller services
            if (descriptor.getControllerServiceDefinition() == null) {
                continue;
            }

            // Convert versioned ID to instance ID if available
            final String valueToApply = versionedIdToInstanceId.getOrDefault(snapshotValue, snapshotValue);
            final String currentValue = component.getRawPropertyValue(descriptor);

            // Only update if the value has changed
            if (!Objects.equals(currentValue, valueToApply)) {
                updates.put(propertyName, valueToApply);
            }
        }

        if (!updates.isEmpty()) {
            component.setProperties(updates, false, Collections.emptySet());
        }
    }

    private void resolveInheritedControllerServices(final FlowSnapshotContainer flowSnapshotContainer, final VersionedProcessGroup versionedGroup,
                                                    final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                    final Stack<Set<VersionedControllerService>> serviceHierarchyStack, //NOPMD
                                                    final Set<String> unresolvedServices) {

        final Set<VersionedControllerService> currentGroupServices = versionedGroup.getControllerServices() == null ? Collections.emptySet() : versionedGroup.getControllerServices();
        serviceHierarchyStack.push(currentGroupServices);

        final Set<VersionedControllerService> availableControllerServices = serviceHierarchyStack.stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        for (final VersionedProcessor processor : versionedGroup.getProcessors()) {
            resolveInheritedControllerServices(processor, currentGroupServices, availableControllerServices, externalControllerServiceReferences, unresolvedServices);
        }

        for (final VersionedControllerService service : versionedGroup.getControllerServices()) {
            resolveInheritedControllerServices(service, currentGroupServices, availableControllerServices, externalControllerServiceReferences, unresolvedServices);
        }

        // If the child group is under version, the external service references need to come from the snapshot of the
        // child instead of what was passed into this method which was for the parent group
        for (final VersionedProcessGroup child : versionedGroup.getProcessGroups()) {
            final Map<String, ExternalControllerServiceReference> childExternalServices;
            if (child.getVersionedFlowCoordinates() == null) {
                childExternalServices = externalControllerServiceReferences;
            } else {
                final RegisteredFlowSnapshot childSnapshot = flowSnapshotContainer.getChildSnapshot(child.getIdentifier());
                if (childSnapshot == null) {
                    childExternalServices = Collections.emptyMap();
                } else {
                    childExternalServices = childSnapshot.getExternalControllerServices();
                }
            }
            resolveInheritedControllerServices(flowSnapshotContainer, child, childExternalServices, serviceHierarchyStack, unresolvedServices);
        }

        serviceHierarchyStack.pop();
    }

    private void resolveInheritedControllerServices(final VersionedConfigurableExtension component,
                                                    final Set<VersionedControllerService> currentGroupServices,
                                                    final Set<VersionedControllerService> availableControllerServices,
                                                    final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                    final Set<String> unresolvedServices) {

        final Map<String, ControllerServiceAPI> componentRequiredApis = controllerServiceApiLookup.getRequiredServiceApis(component.getType(), component.getBundle());
        if (componentRequiredApis.isEmpty()) {
            return;
        }

        final Map<String, String> componentProperties = component.getProperties();

        // Iterate over properties that require controller services (from componentRequiredApis)
        // This avoids the mismatch between property names and descriptor keys after migration
        for (final Map.Entry<String, ControllerServiceAPI> apiEntry : componentRequiredApis.entrySet()) {
            final String propertyName = apiEntry.getKey();
            final ControllerServiceAPI descriptorRequiredApi = apiEntry.getValue();
            final String propertyValue = componentProperties.get(propertyName);

            // if the property isn't set there is nothing to resolve
            if (propertyValue == null) {
                continue;
            }

            // Check if the service is in the current group (versioned ID references are valid)
            final Set<String> currentGroupServiceIds = currentGroupServices.stream()
                    .map(VersionedControllerService::getIdentifier)
                    .collect(Collectors.toSet());

            if (currentGroupServiceIds.contains(propertyValue)) {
                continue;
            }

            // Check if an external reference exists for this ID
            final ExternalControllerServiceReference externalServiceReference = externalControllerServiceReferences == null ? null : externalControllerServiceReferences.get(propertyValue);
            if (externalServiceReference == null) {
                // No external reference, but the ID might be an instance ID already
                // Check if any available service has this as an instance ID
                final boolean foundByInstanceId = availableControllerServices.stream()
                        .anyMatch(service -> propertyValue.equals(service.getInstanceIdentifier()));

                if (foundByInstanceId) {
                    continue;
                }

                unresolvedServices.add(propertyValue);
                continue;
            }

            final String externalControllerServiceName = externalServiceReference.getName();
            final List<VersionedControllerService> matchingControllerServices = availableControllerServices.stream()
                    .filter(service -> service.getName().equals(externalControllerServiceName))
                    .filter(service -> implementsApi(descriptorRequiredApi, service))
                    .toList();

            if (matchingControllerServices.size() != 1) {
                unresolvedServices.add(propertyValue);
                continue;
            }

            final VersionedControllerService matchingService = matchingControllerServices.get(0);
            // For snapshot-based resolution, always use the versioned identifier
            componentProperties.put(propertyName, matchingService.getIdentifier());
        }
    }

    private boolean implementsApi(final ControllerServiceAPI requiredServiceApi, final VersionedControllerService versionedControllerService) {
        if (versionedControllerService.getControllerServiceApis() == null) {
            return false;
        }

        for (final ControllerServiceAPI implementedApi : versionedControllerService.getControllerServiceApis()) {
            if (implementedApi.getType().equals(requiredServiceApi.getType())
                    && implementedApi.getBundle().getGroup().equals(requiredServiceApi.getBundle().getGroup())
                    && implementedApi.getBundle().getArtifact().equals(requiredServiceApi.getBundle().getArtifact())) {
                return true;
            }
        }

        return false;
    }

}
