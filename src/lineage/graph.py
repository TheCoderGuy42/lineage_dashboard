from collections import deque

class LineageGraph: 
  def __init__(self):
    self.edges = set() # holds 
    self.nodes = set()
    self.adjacency_list = {}

  def batch_ingest(self, file):
    lines = file.readlines()
    for line in lines:
      args = line.split(",")
      self.add_edge(*args)
      
  def add_edge(self, src, dest, edge_type="transform"):
    if src == dest:
      raise ValueError("No self referential nodes ")

    edge = (src, dest, edge_type)

    # making sure that the adjacency list isn't empy
    if self.adjacency_list.get(src) == None:
      self.adjacency_list[src] = []

    # making sure you're not duplicating edges
    if dest not in self.adjacency_list[src]:
      self.adjacency_list[src].append(dest)

    self.nodes.add(src)
    self.nodes.add(dest)

    self.edges.add(edge)

  def has_path(self, src, dest):
    if src not in self.nodes or dest not in self.nodes:
      return False

    visited = set()
    stack = deque([src])

    while stack:
      looking = stack.popleft()
      if looking == dest:
        return True
      if looking in visited:
        continue 
      visited.add(looking)
      if self.adjacency_list.get(looking):  
        for d in self.adjacency_list[looking]:
          stack.append(d)
    return False

  def to_dot(self):
    dot = ["digraph"]
    for v1, v2 in self.adjacency_list.items():
      dot.append(f"{v1} -> {"\n".join(v2)}")
    return dot





      
  

       
  
    

  

def main():
  pass

if __name__ == "__main__":
  main()