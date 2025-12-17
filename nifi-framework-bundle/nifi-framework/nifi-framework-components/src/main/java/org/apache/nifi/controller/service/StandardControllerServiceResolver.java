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
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    @Override
    public LiveControllerServiceResolutionPlan planLiveControllerServiceResolutions(final FlowSnapshotContainer flowSnapshotContainer,
                                                                                    final String parentGroupId,
                                                                                    final NiFiUser user) {
        final ProcessGroup root = flowManager.getGroup(parentGroupId);
        if (root == null) {
            return new LiveControllerServiceResolutionPlan(Map.of(), Map.of(), Set.of());
        }

        // Build external id -> name mapping from the snapshot and all versioned descendants
        final Map<String, String> externalIdToName = new HashMap<>();
        buildExternalControllerServiceNameMap(flowSnapshotContainer, externalIdToName);

        final Deque<Set<ControllerServiceNode>> ancestorStack = new ArrayDeque<>();

        // Pre-populate with ancestor groups' services (nearest parent first), filtered by READ auth
        final List<ProcessGroup> ancestors = new ArrayList<>();
        ProcessGroup parent = root.getParent();
        while (parent != null) {
            ancestors.add(parent);
            parent = parent.getParent();
        }
        for (int i = ancestors.size() - 1; i >= 0; i--) {
            final ProcessGroup ancestor = ancestors.get(i);
            final Set<ControllerServiceNode> readable = ancestor.getControllerServices(false).stream()
                    .filter(svc -> svc.isAuthorized(authorizer, RequestAction.READ, user))
                    .collect(Collectors.toCollection(HashSet::new));
            ancestorStack.push(readable);
        }

        final Map<String, Map<String, String>> processorUpdates = new HashMap<>();
        final Map<String, Map<String, String>> controllerServiceUpdates = new HashMap<>();

        findLiveResolutionsPlanRecursive(root, ancestorStack, externalIdToName, user, processorUpdates, controllerServiceUpdates);
        // TODO: Pass the computed set of unresolved identifiers
        return new LiveControllerServiceResolutionPlan(processorUpdates, controllerServiceUpdates, Set.of());
    }

    private void findLiveResolutionsPlanRecursive(final ProcessGroup group,
                                                  final Deque<Set<ControllerServiceNode>> ancestorStack,
                                                  final Map<String, String> externalIdToName,
                                                  final NiFiUser user,
                                                  final Map<String, Map<String, String>> processorUpdates,
                                                  final Map<String, Map<String, String>> controllerServiceUpdates) {
        // Resolve current group components using only ancestors
        for (final ProcessorNode processorNode : group.getProcessors()) {
            final Map<String, String> updates = findServiceReferenceUpdatesLevelAware(processorNode, ancestorStack, externalIdToName);
            if (!updates.isEmpty()) {
                processorUpdates.put(processorNode.getIdentifier(), updates);
            }
        }

        for (final ControllerServiceNode serviceNode : group.getControllerServices(false)) {
            final Map<String, String> updates = findServiceReferenceUpdatesLevelAware(serviceNode, ancestorStack, externalIdToName);
            if (!updates.isEmpty()) {
                controllerServiceUpdates.put(serviceNode.getIdentifier(), updates);
            }
        }

        // Push current group's services (filtered by READ) before recursing into children
        final Set<ControllerServiceNode> currentServices = group.getControllerServices(false).stream()
                .filter(svc -> svc.isAuthorized(authorizer, RequestAction.READ, user))
                .collect(Collectors.toCollection(HashSet::new));
        ancestorStack.push(currentServices);

        for (final ProcessGroup child : group.getProcessGroups()) {
            findLiveResolutionsPlanRecursive(child, ancestorStack, externalIdToName, user, processorUpdates, controllerServiceUpdates);
        }

        ancestorStack.pop();
    }

    private Map<String, String> findServiceReferenceUpdatesLevelAware(final ComponentNode componentNode,
                                                                      final Deque<Set<ControllerServiceNode>> ancestorStack,
                                                                      final Map<String, String> externalIdToName) {
        final Map<String, String> updates = new HashMap<>();

        componentNode.getPropertyDescriptors().forEach(listedDescriptor -> {
            final PropertyDescriptor descriptor = componentNode.getPropertyDescriptor(listedDescriptor.getName());
            if (descriptor == null) {
                return;
            }

            final Class<? extends ControllerService> requiredApi = descriptor.getControllerServiceDefinition();
            if (requiredApi == null) {
                return;
            }

            final String rawValue = componentNode.getRawPropertyValue(descriptor);
            if (rawValue == null || isParameterized(rawValue)) {
                return;
            }

            // If already points to an existing service in scope, skip
            final boolean exists = ancestorStack.stream().flatMap(Set::stream).anyMatch(svc -> svc.getIdentifier().equals(rawValue));
            if (exists) {
                return;
            }

            final String externalName = externalIdToName.get(rawValue);
            if (externalName == null) {
                return;
            }

            ControllerServiceNode chosen = null;
            for (final Set<ControllerServiceNode> levelServices : ancestorStack) {
                final List<ControllerServiceNode> levelMatches = levelServices.stream()
                        .filter(svc -> externalName.equals(svc.getName()))
                        .filter(svc -> requiredApi.isAssignableFrom(svc.getControllerServiceImplementation().getClass()))
                        .toList();

                if (levelMatches.size() == 1) {
                    chosen = levelMatches.getFirst();
                    break;
                } else if (levelMatches.size() > 1) {
                    // Ambiguous at this level; stop evaluation
                    break;
                }
            }

            if (chosen != null) {
                updates.put(descriptor.getName(), chosen.getIdentifier());
            }
        });

        return updates;
    }

    private boolean isParameterized(final String value) {
        return value.contains("#{") || value.contains("${");
    }

    private void buildExternalControllerServiceNameMap(final FlowSnapshotContainer container, final Map<String, String> idToName) {
        if (container == null) {
            return;
        }

        final RegisteredFlowSnapshot top = container.getFlowSnapshot();
        if (top != null && top.getExternalControllerServices() != null) {
            top.getExternalControllerServices().forEach((id, ref) -> {
                if (ref != null && ref.getName() != null) {
                    idToName.put(id, ref.getName());
                }
            });
        }

        if (top != null && top.getFlowContents() != null && top.getFlowContents().getProcessGroups() != null) {
            addChildExternalServiceNamesRecursively(container, top.getFlowContents(), idToName);
        }
    }

    private void addChildExternalServiceNamesRecursively(final FlowSnapshotContainer container,
                                                         final VersionedProcessGroup parent,
                                                         final Map<String, String> idToName) {
        if (parent.getProcessGroups() == null) {
            return;
        }

        for (final VersionedProcessGroup child : parent.getProcessGroups()) {
            final RegisteredFlowSnapshot childSnap = container.getChildSnapshot(child.getIdentifier());
            if (childSnap != null && childSnap.getExternalControllerServices() != null) {
                childSnap.getExternalControllerServices().forEach((id, ref) -> {
                    if (ref != null && ref.getName() != null) {
                        idToName.put(id, ref.getName());
                    }
                });

                if (childSnap.getFlowContents() != null) {
                    addChildExternalServiceNamesRecursively(container, childSnap.getFlowContents(), idToName);
                }
            } else if (child.getVersionedFlowCoordinates() == null && child.getProcessGroups() != null) {
                addChildExternalServiceNamesRecursively(container, child, idToName);
            }
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
            resolveInheritedControllerServices(processor, availableControllerServices, externalControllerServiceReferences, unresolvedServices);
        }

        for (final VersionedControllerService service : versionedGroup.getControllerServices()) {
            resolveInheritedControllerServices(service, availableControllerServices, externalControllerServiceReferences, unresolvedServices);
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

    private void resolveInheritedControllerServices(final VersionedConfigurableExtension component, final Set<VersionedControllerService> availableControllerServices,
                                                    final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                    final Set<String> unresolvedServices) {

        final Map<String, ControllerServiceAPI> componentRequiredApis = controllerServiceApiLookup.getRequiredServiceApis(component.getType(), component.getBundle());
        if (componentRequiredApis.isEmpty()) {
            return;
        }

        final Map<String, VersionedPropertyDescriptor> propertyDescriptors = component.getPropertyDescriptors();
        final Map<String, String> componentProperties = component.getProperties();

        for (final Map.Entry<String, String> entry : componentProperties.entrySet()) {
            final String propertyName = entry.getKey();
            final String propertyValue = entry.getValue();

            // if the property isn't set there is nothing to resolve
            if (propertyValue == null) {
                continue;
            }

            final VersionedPropertyDescriptor propertyDescriptor = propertyDescriptors.get(propertyName);
            if (propertyDescriptor == null) {
                continue;
            }

            if (!propertyDescriptor.getIdentifiesControllerService()) {
                continue;
            }

            final Set<String> availableControllerServiceIds = availableControllerServices.stream()
                    .map(VersionedControllerService::getIdentifier)
                    .collect(Collectors.toSet());

            // If the referenced Controller Service is available, there is nothing to resolve.
            if (availableControllerServiceIds.contains(propertyValue)) {
                unresolvedServices.add(propertyValue);
                continue;
            }

            final ExternalControllerServiceReference externalServiceReference = externalControllerServiceReferences == null ? null : externalControllerServiceReferences.get(propertyValue);
            if (externalServiceReference == null) {
                unresolvedServices.add(propertyValue);
                continue;
            }

            final ControllerServiceAPI descriptorRequiredApi = componentRequiredApis.get(propertyName);
            if (descriptorRequiredApi == null) {
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
            final String resolvedId = matchingService.getIdentifier();
            componentProperties.put(propertyName, resolvedId);
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
