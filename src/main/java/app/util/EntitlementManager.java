package app.util;

import static app.util.EntitlementManager.LinkType.GROUP_RESOURCE;
import static app.util.EntitlementManager.LinkType.PARENT;
import static app.util.EntitlementManager.LinkType.USER_GROUP;
import static app.util.EntitlementManager.NodeType.GROUP;
import static app.util.EntitlementManager.NodeType.RESOURCE;
import static app.util.EntitlementManager.NodeType.USER;
import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;

@Log4j2
public class EntitlementManager {

  @Getter
  private final Nodes<NodeType> nodes = new Nodes<>();

  private final Map<String, Collection<String>> cachedResourcesPerUserId = new Object2ObjectHashMap<>();

  public EntitlementManager() {

  }

  public EntitlementManager(EntitlementConfig config) {
    loadConfig(config);
  }

  public void loadConfig(EntitlementConfig config) {
    if (config.getGroupUsers() != null) {
      config.getGroupUsers().forEach((groupId, userIds) -> {
        for (String userId : userIds) {
          addGroupUser(groupId, userId);
        }
      });
    }
    if (config.getGroupResources() != null) {
      config.getGroupResources().forEach((groupId, resourceNames) -> {
        for (String resourceName : resourceNames) {
          addGroupResource(groupId, resourceName);
        }
      });
    }
    if (config.getSubGroups() != null) {
      config.getSubGroups().forEach((groupId, subGroupNames) -> {
        for (String subGroupName : subGroupNames) {
          addSubGroup(groupId, subGroupName);
        }
      });
    }
    clearCache();
  }

  public void addGroupUser(String groupId, String userId) {
    Node userNode = nodes.getNode(USER, userId);
    Node groupNode = nodes.getNode(GROUP, groupId);
    userNode.link(USER_GROUP, groupNode);
  }

  public void addGroupResource(String groupId, String resourceName) {
    Node groupNode = nodes.getNode(GROUP, groupId);
    Node resourceNode = nodes.getNode(RESOURCE, resourceName);
    groupNode.link(GROUP_RESOURCE, resourceNode);
  }

  public void addSubGroup(String parentGroupId, String subGroupId) {
    Node parentGroupNode = nodes.getNode(GROUP, parentGroupId);
    Node subGroupNode = nodes.getNode(GROUP, subGroupId);
    subGroupNode.link(PARENT, parentGroupNode);
  }

  public Collection<String> getUserGroups(String userId) {
    Set<String> groupIds = new ObjectHashSet<>();
    getUserGroups(userId, true, groupIds::add);
    return groupIds;
  }

  public Collection<String> getGroupResources(String groupId) {
    Set<String> resourceNames = new ObjectHashSet<>();
    getGroupResources(groupId, (resourceName, include) -> {
      if (include) {
        resourceNames.add(resourceName);
      }
    });
    getGroupResources(groupId, (resourceName, include) -> {
      if (!include) {
        resourceNames.remove(resourceName);
      }
    });
    return resourceNames;
  }

  public Collection<String> getCachedUserResources(String userId) {
    return cachedResourcesPerUserId.computeIfAbsent(userId, this::getUserResources);
  }

  public Collection<String> getUserResources(String userId) {
    Set<String> resourceNames = new ObjectHashSet<>();
    getUserGroups(userId, true, groupId -> {
      getGroupResources(groupId, (resourceName, include) -> {
        if (include) {
          resourceNames.add(resourceName);
        }
      });
    });
    getUserGroups(userId, true, groupId -> {
      getGroupResources(groupId, (resourceName, include) -> {
        if (!include) {
          resourceNames.remove(resourceName);
        }
      });
    });
    return resourceNames;
  }

  public void getParentGroups(Node groupNode, Consumer<Node> nodeConsumer) {
    Queue<Node> nodeQueue = new LinkedList<>();
    Set<Node> walkedNodes = new HashSet<>();
    nodeQueue.add(groupNode);
    while (!nodeQueue.isEmpty()) {
      Node node = nodeQueue.poll();
      if (walkedNodes.contains(node)) {
        log.warn("Node {} has already been traversed when scanning parent groups",
            node.getName());
      } else {
        if (node != groupNode) {
          nodeConsumer.accept(node);
        }
        walkedNodes.add(node);
        Map<String, Node> parentNodes = node.getOutboundNodes(PARENT, false);
        if (parentNodes != null) {
          nodeQueue.addAll(parentNodes.values());
        }
      }
    }
  }

  public void getUserGroups(String userId, boolean includeParentGroup, Consumer<String> groupIdConsumer) {
    Node userNode = nodes.getNode(USER, userId);
    Map<String, Node> groupNodeMap = userNode.getOutboundNodes(USER_GROUP);
    groupNodeMap.forEach((groupId, groupNode) -> {
      groupIdConsumer.accept(groupId);
      if (includeParentGroup) {
        getParentGroups(groupNode, node -> {
          groupIdConsumer.accept(node.getName());
        });
      }
    });
  }

  public void getGroupResources(String groupId, IncludeOrExcludeNameConsumer resourceNameConsumer) {
    Node groupNode = nodes.getNode(GROUP, groupId);
    Map<String, Node> resourceNodeMap = groupNode.getOutboundNodes(GROUP_RESOURCE);
    if (resourceNodeMap != null) {
      for (String resourceName : resourceNodeMap.keySet()) {
        boolean include = resourceName.charAt(0) != '!';
        String actualName = include ? resourceName : resourceName.substring(1);
        resourceNameConsumer.accept(actualName, include);
      }
    }
  }

  public void reset() {
    nodes.clear();
  }

  public void clearCache() {
    cachedResourcesPerUserId.forEach((userId, resourceNames) -> resourceNames.clear());
    cachedResourcesPerUserId.clear();
  }

  public enum NodeType {

    USER, GROUP, RESOURCE

  }

  public enum LinkType {

    USER_GROUP, GROUP_RESOURCE, PARENT

  }

  public interface IncludeOrExcludeNameConsumer {
    void accept(String name, boolean includeOrExclude);
  }

  @Data
  public static class Nodes<T> {

    private final Int2ObjectHashMap<Node> nodeMap = new Int2ObjectHashMap<>();

    private final Node rootNode = Node.of("root");

    public Node getNode(T type, String name) {
      int hash = Objects.hash(type, name);
      Node node = nodeMap.computeIfAbsent(hash, k -> Node.of(name));
      rootNode.link(type, node);
      return node;
    }

    public Collection<Node> getNodes(T type) {
      Map<String,Node> outboundNodesPerType = rootNode.getOutboundNodes(type, false);
      return outboundNodesPerType == null ? emptyList() : outboundNodesPerType.values();
    }

    public void clear() {
      nodeMap.forEach((hash, node) -> {
        node.unlinkAll();
        nodeMap.remove(hash);
      });
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      AtomicInteger typeSeq = new AtomicInteger();
      rootNode.getOutboundNodesPerType().forEach((type, subMap) -> {
        if (typeSeq.getAndIncrement() > 0) {
          sb.append('\n');
        }
        sb.append("[").append(type).append("]");
        for (Node node : subMap.values()) {
          sb.append('\n').append(node);
        }
      });
      return sb.toString();
    }

  }

  @Data
  public static class EntitlementConfig {

    private Map<String, List<String>> groupUsers;

    private Map<String, List<String>> groupEntitlements;

    private Map<String, List<String>> groupResources;

    private Map<String, List<String>> subGroups;

  }

  @Data
  @EqualsAndHashCode(onlyExplicitlyIncluded = true)
  @RequiredArgsConstructor(staticName = "of")
  public static class Node {

    @EqualsAndHashCode.Include
    private final String name;

    @ToString.Exclude
    private final Map<String, Map<String, Node>> inboundNodesPerType = new Object2ObjectHashMap<>();

    private final Map<String, Map<String, Node>> outboundNodesPerType = new Object2ObjectHashMap<>();

    public <T> Node link(T type, Node node) {
      getOutboundNodes(type, true).put(node.getName(), node);
      node.getInboundNodes(type, true).put(name, this);
      return this;
    }

    public <T> Node unlink(T type, Node node) {
      Map<String, Node> outboundNodesForType = getOutboundNodes(type, false);
      Map<String, Node> inboundNodesForType = node.getInboundNodes(type, false);
      if (outboundNodesForType != null) {
        outboundNodesForType.remove(node.getName());
        if (outboundNodesForType.isEmpty()) {
          outboundNodesPerType.remove(type.toString());
        }
      }
      if (inboundNodesForType != null) {
        inboundNodesForType.remove(name);
        if (inboundNodesForType.isEmpty()) {
          node.getInboundNodesPerType().remove(type.toString());
        }
      }
      return this;
    }

    public <T> void unlinkAll(T type) {
      Map<String, Node> outboundNodesForType = getOutboundNodes(type, false);
      Map<String, Node> inboundNodesForType = getInboundNodes(type, false);
      if (outboundNodesForType != null) {
        for (Node node : outboundNodesForType.values()) {
          unlink(type, node);
        }
      }
      if (inboundNodesForType != null) {
        for (Node node : inboundNodesForType.values()) {
          node.unlink(type, this);
        }
      }
    }

    public void unlinkAll() {
      for (String type : outboundNodesPerType.keySet()) {
        unlinkAll(type);
      }
      for (String type : inboundNodesPerType.keySet()) {
        unlinkAll(type);
      }
    }

    public <T> Node getInboundNode(T type, String name) {
      return getLinkedNode(type.toString(), name, inboundNodesPerType);
    }

    public <T> Node getOutboundNode(T type, String name) {
      return getLinkedNode(type.toString(), name, outboundNodesPerType);
    }

    public <T> Map<String, Node> getInboundNodes(T type) {
      return getInboundNodes(type, false);
    }

    public <T> Map<String, Node> getInboundNodes(T type, boolean createIfAbsent) {
      return getLinkedNodes(type.toString(), createIfAbsent, inboundNodesPerType);
    }

    public <T> Map<String, Node> getOutboundNodes(T type) {
      return getOutboundNodes(type, false);
    }

    public <T> Map<String, Node> getOutboundNodes(T type, boolean createIfAbsent) {
      return getLinkedNodes(type.toString(), createIfAbsent, outboundNodesPerType);
    }

    private Node getLinkedNode(String type, String name, Map<String, Map<String, Node>> nodeMapsPerType) {
      Map<String, Node> nodeMap = getLinkedNodes(type, false, nodeMapsPerType);
      return nodeMap == null ? null : nodeMap.get(name);
    }

    private Map<String, Node> getLinkedNodes(String type, boolean createIfAbsent,
        Map<String, Map<String, Node>> nodeMapsPerType) {
      return nodeMapsPerType.compute(type, (k, v) -> v == null && createIfAbsent ?
          new Object2ObjectHashMap<>() : v);
    }

    @Override
    public String toString() {
      AtomicInteger seq = new AtomicInteger();
      StringBuilder sb = new StringBuilder();
      sb.append(name).append(':');
      outboundNodesPerType.forEach((type, subMap) -> {
        if (seq.getAndIncrement() > 0) {
          sb.append(";");
        }
        sb.append(' ').append(type).append(" -> ").append(String.join(", ", subMap.keySet()));
      });
      inboundNodesPerType.forEach((type, subMap) -> {
        if (seq.getAndIncrement() > 0) {
          sb.append(";");
        }
        sb.append(' ').append(type).append(" <- ").append(String.join(", ", subMap.keySet()));
      });
      return sb.toString();
    }

  }

}
