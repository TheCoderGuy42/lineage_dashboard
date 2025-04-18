from pathlib import Path
import pandas as pd
from typing import Dict, List
from lineage.graph import LineageGraph





def add_column_lineage(
        graph: LineageGraph,
        src_file: Path,
        dest_file: Path,
        rename_map: Dict[str, str] | None = None,
        edge_type: str = "transform",
) -> List[tuple[str, str]]:
    """
    Read two CSVs.
    For every column that matches (after rename_map applied) add
    an edge src::<col> -> dest::<mapped_col>.
    Return list of edges added (for tests).
    """

    src = pd.read_csv(src_file)
    dest = pd.read_csv(dest_file)


    
    src_columns = src.columns.values.tolist()
    dest_columns = dest.columns.values.tolist()


    for col in src_columns:

      new_col = col
      
      if rename_map and rename_map.get(col) != None:
        new_col = rename_map[col]

      if new_col not in dest_columns: continue
      
      src_node = node_id(src_file, col)
      dst_node = node_id(dest_file, new_col)

      graph.add_edge(src_node, dst_node, edge_type)


def node_id (file_path: Path, col):
   return f"{file_path.name}::{col}"
