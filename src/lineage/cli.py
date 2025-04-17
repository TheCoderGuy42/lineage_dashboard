import argparse, sys
from lineage.graph import LineageGraph

graph = LineageGraph()

def cmd_add(args):
    graph.add_edge(args.src, args.dest, args.type)

def cmd_path(args):
    print(graph.has_path(args.src, args.dest))

def main():
    p = argparse.ArgumentParser(prog="lineage")
    sub = p.add_subparsers(dest="command")
    sub.required = True                # Python 3.7+ makes sub‑command mandatory

    add = sub.add_parser("add-edge")
    add.add_argument("src")
    add.add_argument("dest")
    add.add_argument("--type", default="transform")
    add.set_defaults(func=cmd_add)

    path = sub.add_parser("has-path")
    path.add_argument("src")
    path.add_argument("dest")
    path.set_defaults(func=cmd_path)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()