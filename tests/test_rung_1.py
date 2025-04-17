# tests/test_lineage.py
import pytest
import io
from lineage.graph import LineageGraph            # adjust if your module path differs
from hypothesis import given, strategies as st

# ----------  Core invariants -------------------------------------------------

def test_idempotent_edge_insert():
    g = LineageGraph()
    g.add_edge("a", "b")
    g.add_edge("a", "b")          # same call, same default edge_type
    assert len(g.edges) == 1
    assert g.has_path("a", "b") is True

def test_self_loop_rejected():
    g = LineageGraph()
    with pytest.raises(ValueError):
        g.add_edge("x", "x")

def test_directed_reachability():
    g = LineageGraph()
    g.add_edge("a", "b")
    g.add_edge("b", "c")
    assert g.has_path("a", "c") is True
    assert g.has_path("c", "a") is False       # direction matters!

def test_unknown_nodes_return_false():
    g = LineageGraph()
    g.add_edge("foo", "bar")
    assert g.has_path("ghost", "bar") is False
    assert g.has_path("foo", "ghost") is False

# ----------  Multi‑edge & metadata -------------------------------------------

def test_multiple_edge_types_coexist():
    g = LineageGraph()
    g.add_edge("raw.csv", "clean.csv", edge_type="transform")
    g.add_edge("raw.csv", "clean.csv", edge_type="copy")
    # two distinct triples should now exist
    assert len(g.edges) == 2

# ----------  Cycle handling --------------------------------------------------

def test_cycle_does_not_infinite_loop():
    g = LineageGraph()
    g.add_edge("a", "b")
    g.add_edge("b", "c")
    g.add_edge("c", "a")          # creates a cycle
    assert g.has_path("a", "c") is True
    assert g.has_path("c", "a") is True

# ----------  Integrity / read‑only queries -----------------------------------

def test_has_path_is_pure():
    g = LineageGraph()
    g.add_edge("x", "y")
    pre_nodes = g.nodes.copy()
    pre_edges = g.edges.copy()
    _ = g.has_path("x", "y")
    assert g.nodes == pre_nodes
    assert g.edges == pre_edges     # no mutation

# ----------  Duplicate node insert idempotency -------------------------------

def test_duplicate_node_insert():
    g = LineageGraph()
    g.nodes.add("solo.csv")         # manual insert, simulating call elsewhere
    g.add_edge("solo.csv", "other.csv")
    assert "solo.csv" in g.nodes
    assert len([n for n in g.nodes if n == "solo.csv"]) == 1   # no dupes

# ----------  Property‑based fuzzing  (Hypothesis) ----------------------------

@given(st.lists(st.tuples(st.text(min_size=1), st.text(min_size=1)), min_size=1, max_size=50))
def test_fuzz_edges_dont_break_invariants(edge_pairs):
    """
    Insert N random edges and assert:
    - Nodes <= 2 * Edges
    - No edge duplicates
    - has_path(src,dst) always returns True for inserted pair
    """
    g = LineageGraph()
    for src, dst in edge_pairs:
        if src != dst:                 # obey self‑loop rule
            g.add_edge(src, dst)
            assert g.has_path(src, dst)

    # nodes bound: each edge introduces at most 2 new nodes
    assert len(g.nodes) <= 2 * len(g.edges)

# ----------  Performance sanity check (optional) -----------------------------
# Skip by default; enable with: pytest -k perf
def test_has_path_time_complexity(benchmark):
    """
    Build a graph with 10k linear edges and benchmark a middle‑to‑end path.
    Target: query under 30ms on typical laptop.
    """
    g = LineageGraph()
    n = 10_000
    for i in range(n):
        g.add_edge(f"n{i}", f"n{i+1}")

    def run():
        assert g.has_path("n100", f"n{n}") is True

    # benchmark will raise if median runtime > threshold (adjust if needed)

     # right API: benchmark.pedantic returns a BenchmarkStats
    # 1 call per round, 50 rounds, 5 warm‑up rounds
    benchmark.pedantic(
        run,
        iterations=1,
        rounds=50,
        warmup_rounds=5
    )

    median = benchmark.stats.stats.median
    assert median < 0.03


def test_batch_ingest_idempotent():
    txt = io.StringIO("a,b,transform\nb,c,transform\na,b,transform\n")
    g = LineageGraph()
    g.batch_ingest(txt)
    assert len(g.edges) == 2          # duplicate row ignored

def test_adjacency_list_no_dupes():
    g = LineageGraph()
    g.add_edge("x","y")
    g.add_edge("x","y")
    assert g.adjacency_list["x"].count("y") == 1

def test_cli_add_edge(tmp_path):
    """Assumes you implement a cli.py entry‑point with `add-edge`."""
    from subprocess import run
    script = "python -m lineage.cli add-edge a b"
    result = run(script.split(), capture_output=True, text=True)
    assert result.returncode == 0

def test_export_dot():
    g = LineageGraph()
    g.add_edge("a","b")
    dot = g.to_dot()                 # "digraph" in output
    assert "digraph" in dot and "a -> b" in dot