"""Generate the console's TypeScript types from the running API contract."""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

from fluxplane.api.app import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if committed types are stale")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    web = root / "apps" / "web"
    committed = web / "src" / "lib" / "generated"
    cli = web / "node_modules" / ".bin" / "openapi-ts"
    if not cli.is_file():
        raise SystemExit("install frontend dependencies with npm ci before generating types")
    with tempfile.TemporaryDirectory() as directory:
        temporary = Path(directory)
        specification = temporary / "openapi.json"
        specification.write_text(json.dumps(create_app().openapi(), sort_keys=True))
        output = temporary / "generated" if args.check else committed
        subprocess.run([str(cli), "-i", str(specification), "-o", str(output),
                        "-p", "@hey-api/typescript", "--no-log-file"],
                       cwd=web, check=True)
        if args.check:
            files = {path.name for path in output.iterdir()}
            if files != {path.name for path in committed.iterdir()} or any(
                (output / name).read_bytes() != (committed / name).read_bytes()
                for name in files
            ):
                raise SystemExit("generated API types are stale; run npm run generate:api")


if __name__ == "__main__":
    main()
