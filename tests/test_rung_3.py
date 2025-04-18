# tests/test_rung_3.py
import os, sqlite3, pytest, threading
from pathlib import Path
from lineage.graph import LineageGraph

# ----------  Helpers ---------------------------------------------------------

def open_sqlite(path: Path):
    return sqlite3.connect(path, timeout=1.0)

def count_rows(path: Path):
    with open_sqlite(path) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM edges")
        return cur.fetchone()[0]

# ----------  Basic persistence ----------------------------------------------

def test_persist_single_edge(tmp_path):
    db = tmp_path / "graph.db"
    g = LineageGraph(store=f"sqlite:///{db}")
    g.add_edge("a", "b")
    g.close()                     # or g.__exit__ if ctx mgr

    g2 = LineageGraph(store=f"sqlite:///{db}")
    assert g2.has_path("a", "b") is True
    g2.close()

def test_unique_constraint(tmp_path):
    db = tmp_path / "graph.db"
    g = LineageGraph(store=f"sqlite:///{db}")
    g.add_edge("a", "b")
    g.add_edge("a", "b")          # same triple twice
    g.close()

    assert count_rows(db) == 1    # DB enforced uniqueness

def test_multi_edge_types(tmp_path):
    db = tmp_path / "graph.db"
    g = LineageGraph(store=f"sqlite:///{db}")
    g.add_edge("x", "y", "transform")
    g.add_edge("x", "y", "copy")
    g.close()
    assert count_rows(db) == 2

def test_schema_auto_created(tmp_path):
    db = tmp_path / "auto.db"
    LineageGraph(store=f"sqlite:///{db}").close()
    assert db.exists() and db.stat().st_size > 0

# ----------  Read consistency across sessions --------------------------------

def test_multiple_sessions_see_each_other(tmp_path):
    db = tmp_path / "graph.db"
    g1 = LineageGraph(store=f"sqlite:///{db}")
    g2 = LineageGraph(store=f"sqlite:///{db}")

    g1.add_edge("a", "b")
    g2.add_edge("b", "c")
    assert g1.has_path("a", "c") is True   # via shared DB
    g1.close(); g2.close()

# ----------  Idempotency after reopen ---------------------------------------

def test_duplicate_insert_after_reopen(tmp_path):
    db = tmp_path / "graph.db"
    g = LineageGraph(store=f"sqlite:///{db}")
    g.add_edge("p", "q")
    g.close()

    g2 = LineageGraph(store=f"sqlite:///{db}")
    g2.add_edge("p", "q")
    g2.close()
    assert count_rows(db) == 1

# ----------  Memory vs SQLite parity ----------------------------------------

def test_in_memory_and_sqlite_same_api(tmp_path):
    sqlite_g = LineageGraph(store=f"sqlite:///{tmp_path/'db.sqlite'}")
    mem_g    = LineageGraph()                   # default in‑mem

    for g in (sqlite_g, mem_g):
        g.add_edge("s", "t")
        g.add_edge("t", "u")
        assert g.has_path("s", "u")
        g.close()

# ----------  Concurrency smoke test -----------------------------------------

def test_threaded_inserts(tmp_path):
    db = tmp_path / "threaded.db"
    def worker(src):
        g = LineageGraph(store=f"sqlite:///{db}")
        for i in range(100):
            g.add_edge(src, f"{src}_{i}")
        g.close()

    th1 = threading.Thread(target=worker, args=("x",))
    th2 = threading.Thread(target=worker, args=("y",))
    th1.start(); th2.start(); th1.join(); th2.join()

    assert count_rows(db) == 200

# ----------  Transaction rollback stub (optional / advanced) -----------------

@pytest.mark.xfail(reason="rollback handling not yet implemented")
def test_atomic_failure_rolls_back(tmp_path, monkeypatch):
    """
    Patch add_edge to throw mid‑transaction and assert no partial row.
    Requires your implementation to wrap inserts in a tx.
    """
    db = tmp_path / "fail.db"
    g  = LineageGraph(store=f"sqlite:///{db}")

    # Simulate low‑level DB failure on second execute
    orig = g._store._conn.execute
    counter = {"n": 0}
    def flaky(sql, *params):
        counter["n"] += 1
        if counter["n"] == 2:
            raise sqlite3.OperationalError("boom")
        return orig(sql, *params)
    monkeypatch.setattr(g._store._conn, "execute", flaky)

    with pytest.raises(sqlite3.OperationalError):
        g.add_edge("a", "b")      # success
        g.add_edge("b", "c")      # boom

    g.close()
    assert count_rows(db) == 0    # nothing persisted
