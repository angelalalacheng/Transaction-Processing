from collections import defaultdict

class DependencyGraph:
    def __init__(self):
        self.graph = defaultdict(list)

    def add_edge(self, from_node, to_node):
        self.graph[from_node].append(to_node)

    def is_cyclic_util(self, node, visited, rec_stack):
        visited.add(node)
        rec_stack.add(node)
        for neighbor in self.graph[node]:
            if neighbor not in visited:
                if self.is_cyclic_util(neighbor, visited, rec_stack):
                    return True
                elif neighbor in rec_stack:
                    return True
        rec_stack.remove(node)
        return False

    def is_cyclic(self):
        visited = set()
        rec_stack = set()
        for node in list(self.graph):
            if node not in visited:
                if self.is_cyclic_util(node, visited, rec_stack):
                    return True
        return False

