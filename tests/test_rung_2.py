# tests/test_rung_2.py
import io, pytest
from pathlib import Path
from lineage.graph import LineageGraph
from lineage.column_ingest import add_column_lineage

RAW_1 = io.StringIO("id,total,timestamp\n1,10,2025-01-01\n")
CLEAN_1 = io.StringIO("order_id,total,timestamp\n1,10,2025-01-01\n")
EXTRA   = io.StringIO("order_id,total,timestamp,discount\n1,10,2025-01-01,0\n")

def write(tmp, name, buf):
    p = tmp / name; p.write_text(buf.getvalue()); return p

def test_basic_mapping(tmp_path):
    g = LineageGraph()
    src = write(tmp_path, "raw.csv",   RAW_1)
    dst = write(tmp_path, "clean.csv", CLEAN_1)
    add_column_lineage(g, src, dst, rename_map={"id": "order_id"})
    assert len(g.edges) == 3
    assert g.has_path("raw.csv::id", "clean.csv::order_id")

def test_missing_column_ignored(tmp_path):
    g = LineageGraph()
    src = write(tmp_path, "raw.csv", RAW_1)
    dst = write(tmp_path, "extra.csv", EXTRA)
    add_column_lineage(g, src, dst, rename_map={"id": "order_id"})
    assert len(g.edges) == 3                      # 'discount' skipped

def test_idempotent(tmp_path):
    g   = LineageGraph()
    src = write(tmp_path, "raw.csv", RAW_1)
    dst = write(tmp_path, "clean.csv", CLEAN_1)
    for _ in range(2):
        add_column_lineage(g, src, dst, rename_map={"id": "order_id"})
    assert len(g.edges) == 3                      # no duplicates

# -----------  New edge‑case tests -------------------------------------------

def test_column_order_irrelevant(tmp_path):
    g = LineageGraph()
    src = write(tmp_path, "raw.csv",
                io.StringIO("id,total,timestamp\n1,10,2025-01-01\n"))
    dst = write(tmp_path, "clean.csv",
                io.StringIO("total,order_id,timestamp\n10,1,2025-01-01\n"))
    add_column_lineage(g, src, dst, rename_map={"id": "order_id"})
    assert g.has_path("raw.csv::total", "clean.csv::total")
    assert g.has_path("raw.csv::id",    "clean.csv::order_id")

def test_only_common_columns(tmp_path):
    g = LineageGraph()
    src = write(tmp_path, "a.csv", io.StringIO("x,y\n1,2\n"))
    dst = write(tmp_path, "b.csv", io.StringIO("y,z\n2,3\n"))
    add_column_lineage(g, src, dst)
    # Only 'y' survives
    assert len(g.edges) == 1
    assert g.has_path("a.csv::y", "b.csv::y")
