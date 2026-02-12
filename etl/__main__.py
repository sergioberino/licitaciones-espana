"""Allow running the CLI as python -m etl."""

from etl.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
