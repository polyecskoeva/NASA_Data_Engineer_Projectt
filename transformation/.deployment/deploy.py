#!/usr/bin/env python3
"""
Unified Snowflake DDL deployment CLI.

Subcommands:
  detect-changes  Build file list (git diff or find all .sql files)
  deploy          Deploy SQL files with per-file tracking
  last-commit     Print last successful deploy commit SHA (for CI)

Auth: SSO (externalbrowser) locally, key-pair (SNOWFLAKE_JWT) in CI.
Change detection: SHA256 file hash — same content is never redeployed.

Usage:
  python3 .deployment/deploy.py detect-changes [--mode diff|log] [--base-ref REF]
  python3 .deployment/deploy.py deploy [--dry-run] [--file-list FILE]
  python3 .deployment/deploy.py last-commit --branch BRANCH
"""

import argparse
import hashlib
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

COMPONENT = "snowflake"
OUTPUT_FILE = "snowflake_changed_files.txt"
LAST_DEPLOY_FILE = ".snowflake_last_deploy_commit"


def file_hash(path: str) -> str:
    """SHA256 of file content — identifies content regardless of commit."""
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def git(*args: str, check: bool = True, capture: bool = True) -> str:
    """Run a git command, return stripped stdout."""
    result = subprocess.run(
        ["git", *args],
        capture_output=capture,
        text=True,
        check=check,
    )
    return result.stdout.strip() if capture else ""


def snowflake_connect():
    """Build connection params and connect. SSO locally, key-pair in CI."""
    try:
        import snowflake.connector
    except ImportError:
        print("Error: snowflake-connector-python required. Run: pip install snowflake-connector-python", file=sys.stderr)
        sys.exit(1)

    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE")
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"Error: missing env vars: {', '.join(missing)}. Source .env or set in CI.", file=sys.stderr)
        sys.exit(1)

    key_path = os.path.expanduser("~/.snowflake/rsa_key.p8")
    params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
    }
    if os.path.isfile(key_path):
        params["private_key_file"] = key_path
        passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASS") or os.environ.get("PRIVATE_KEY_PASSPHRASE") or ""
        if passphrase:
            params["private_key_file_pwd"] = passphrase
        params["authenticator"] = "SNOWFLAKE_JWT"
    else:
        params["authenticator"] = "externalbrowser"

    return snowflake.connector.connect(**params)


def consume_results(cursor):
    """Drain all result sets from a multi-statement execute."""
    while True:
        try:
            cursor.fetchall()
        except Exception:
            pass
        if not cursor.nextset():
            break


def bootstrap_tables(cursor):
    """Ensure TECH.DEPLOYMENT_HISTORY and TECH.DEPLOYMENT_FILE_HISTORY exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS TECH.DEPLOYMENT_HISTORY (
            DEPLOYMENT_ID VARCHAR(36),
            FOLDER_NAME VARCHAR(100),
            COMMIT_SHA VARCHAR(40),
            DEPLOYMENT_STATUS VARCHAR(20),
            START_TIME TIMESTAMP_NTZ,
            END_TIME TIMESTAMP_NTZ,
            BRANCH_NAME VARCHAR(255)
        );
        CREATE TABLE IF NOT EXISTS TECH.DEPLOYMENT_FILE_HISTORY (
            DEPLOYMENT_ID VARCHAR(36),
            FILE_PATH VARCHAR(500),
            FILE_HASH VARCHAR(64),
            STATUS VARCHAR(20),
            ERROR_MESSAGE VARCHAR(1000),
            DEPLOYED_AT TIMESTAMP_NTZ
        );
        ALTER TABLE TECH.DEPLOYMENT_FILE_HISTORY ADD COLUMN IF NOT EXISTS FILE_HASH VARCHAR(64);
    """, num_statements=0)
    consume_results(cursor)


# ---------------------------------------------------------------------------
# Subcommand: last-commit
# ---------------------------------------------------------------------------

def cmd_last_commit(args):
    """Query Snowflake for the last successful deploy commit on a branch."""
    conn = snowflake_connect()
    try:
        cur = conn.cursor()
        bootstrap_tables(cur)
        cur.execute("""
            SELECT COMMIT_SHA FROM TECH.DEPLOYMENT_HISTORY
            WHERE FOLDER_NAME = %s AND DEPLOYMENT_STATUS = 'SUCCESS' AND BRANCH_NAME = %s
            ORDER BY START_TIME DESC LIMIT 1
        """, (COMPONENT, args.branch))
        row = cur.fetchone()
        if row and row[0]:
            print(row[0])
        else:
            fallback = git("rev-list", "HEAD", "--reverse").splitlines()[0]
            print(f"No successful '{COMPONENT}' deployment found. Using initial commit: {fallback}", file=sys.stderr)
            print(fallback)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Subcommand: detect-changes
# ---------------------------------------------------------------------------

def cmd_detect_changes(args):
    """Build snowflake_changed_files.txt based on mode (diff or log)."""
    try:
        git("config", "--global", "--add", "safe.directory", os.getcwd(), check=False)
    except Exception:
        pass

    mode = args.mode
    base_ref = args.base_ref

    # Auto-detect default branch if no base-ref provided
    if base_ref is None:
        for candidate in ("main", "master", "DEV", "dev"):
            verify = git("rev-parse", "--verify", candidate, check=False)
            if verify:
                base_ref = candidate
                break
        if base_ref is None:
            # Fallback: use origin HEAD
            head_ref = git("symbolic-ref", "refs/remotes/origin/HEAD", check=False)
            if head_ref:
                base_ref = head_ref.replace("refs/remotes/origin/", "")
            else:
                base_ref = "main"  # ultimate fallback
        print(f"Auto-detected base branch: {base_ref}")

    if mode == "log":
        files = sorted(str(p) for p in Path(COMPONENT).rglob("*.sql"))
        Path(OUTPUT_FILE).write_text("\n".join(files) + "\n" if files else "")
        if files:
            print("All files (log-based deploy):")
            for f in files:
                print(f"  - {f}")
        else:
            print(f"No .sql files in {COMPONENT}/")
        return

    # Diff mode: resolve base ref
    if base_ref == "snowflake":
        # CI mode — query Snowflake for last deploy commit
        branch = os.environ.get("BRANCH_NAME") or git("branch", "--show-current")
        try:
            # Reuse last-commit logic inline (avoids opening a second connection)
            conn = snowflake_connect()
            try:
                cur = conn.cursor()
                bootstrap_tables(cur)
                cur.execute("""
                    SELECT COMMIT_SHA FROM TECH.DEPLOYMENT_HISTORY
                    WHERE FOLDER_NAME = %s AND DEPLOYMENT_STATUS = 'SUCCESS' AND BRANCH_NAME = %s
                    ORDER BY START_TIME DESC LIMIT 1
                """, (COMPONENT, branch))
                row = cur.fetchone()
                base_ref = row[0] if row and row[0] else None
            finally:
                conn.close()
        except Exception:
            base_ref = None

        if not base_ref:
            base_ref = git("rev-list", "HEAD", "--reverse").splitlines()[0]
            print(f"No successful deployment found. Using initial commit: {base_ref}", file=sys.stderr)
        else:
            print(f"Last successful deployment on '{branch}': {base_ref}")

        after = os.environ.get("AFTER") or git("rev-parse", "HEAD")
        print(f"Diffing {base_ref[:8]}..{after[:8]}")

        # Verify the base ref is reachable from current HEAD
        merge_base = git("merge-base", base_ref, after, check=False)
        if not merge_base:
            print(f"Warning: base ref {base_ref[:8]} is not reachable from HEAD. Falling back to merge-base with default branch.", file=sys.stderr)
            for default_branch in ("main", "master", "dev"):
                merge_base = git("merge-base", default_branch, after, check=False)
                if merge_base:
                    break
            if merge_base:
                base_ref = merge_base
                print(f"Using merge-base: {base_ref[:8]}")
            else:
                base_ref = git("rev-list", "HEAD", "--reverse").splitlines()[0]
                print(f"Using initial commit: {base_ref[:8]}")

        diff_output = git("diff", "--name-only", "--diff-filter=ACMRTUXB", base_ref, after, check=False)

        if not diff_output:
            print(f"git diff returned no output. Verifying base ref exists...")
            verify = git("cat-file", "-t", base_ref, check=False)
            if verify != "commit":
                print(f"Warning: base ref {base_ref[:8]} not found in history. Falling back to all SQL files.", file=sys.stderr)
                diff_output = "\n".join(str(p) for p in Path(COMPONENT).rglob("*.sql"))
    else:
        # Local mode: prefer last-deploy commit over main
        if base_ref == "main" and Path(LAST_DEPLOY_FILE).is_file():
            last = Path(LAST_DEPLOY_FILE).read_text().strip()
            try:
                git("rev-parse", last)
                base_ref = last
            except Exception:
                pass

        diff_output = git("diff", "--name-only", "--diff-filter=ACMRTUXB", base_ref, "--", f"{COMPONENT}/", check=False)

    files = sorted(
        line for line in diff_output.splitlines()
        if line.startswith(f"{COMPONENT}/") and line.endswith(".sql")
    )

    Path(OUTPUT_FILE).write_text("\n".join(files) + "\n" if files else "")
    if files:
        print("Changed files:")
        for f in files:
            print(f"  - {f}")
    else:
        print(f"No changed files in {COMPONENT}/")


# ---------------------------------------------------------------------------
# Subcommand: deploy
# ---------------------------------------------------------------------------

def cmd_deploy(args):
    """Deploy SQL files to Snowflake with per-file hash tracking."""
    file_list = args.file_list
    branch = args.branch or os.environ.get("BRANCH_NAME") or _git_branch()
    commit = args.commit or _git_head()
    dry_run = args.dry_run

    if not Path(file_list).is_file():
        print(f"Error: File list '{file_list}' not found!")
        print("Run 'make detect-changes' or 'make dry-run' first.")
        sys.exit(1)

    all_files = [
        line.strip() for line in Path(file_list).read_text().splitlines()
        if line.strip().endswith(".sql")
    ]
    all_files.sort()

    if not all_files:
        print("No files to deploy.")
        return

    print(f"Deployment started for folder: '{COMPONENT}'")

    if dry_run:
        _dry_run(all_files)
        return

    deployment_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Deployment ID: {deployment_id}")

    conn = snowflake_connect()
    try:
        cur = conn.cursor()
        bootstrap_tables(cur)

        cur.execute("""
            INSERT INTO TECH.DEPLOYMENT_HISTORY
            (DEPLOYMENT_ID, FOLDER_NAME, COMMIT_SHA, DEPLOYMENT_STATUS, START_TIME, BRANCH_NAME)
            VALUES (%s, %s, %s, 'PENDING', %s, %s)
        """, (deployment_id, COMPONENT, commit, start_time, branch))

        # Query already-deployed (file_path, file_hash) pairs across all branches
        cur.execute("""
            SELECT fh.FILE_PATH, fh.FILE_HASH
            FROM TECH.DEPLOYMENT_FILE_HISTORY fh
            JOIN TECH.DEPLOYMENT_HISTORY h ON fh.DEPLOYMENT_ID = h.DEPLOYMENT_ID
            WHERE h.DEPLOYMENT_ID != %s AND fh.STATUS = 'SUCCESS' AND fh.FILE_HASH IS NOT NULL
        """, (deployment_id,))
        deployed = {(row[0], row[1]) for row in cur.fetchall()}

        files_to_deploy = [
            f for f in all_files
            if os.path.isfile(f) and (f, file_hash(f)) not in deployed
        ]

        if not files_to_deploy:
            print("All files already deployed successfully. Nothing to do.")
            cur.execute(
                "UPDATE TECH.DEPLOYMENT_HISTORY SET DEPLOYMENT_STATUS = 'SUCCESS', END_TIME = CURRENT_TIMESTAMP() WHERE DEPLOYMENT_ID = %s",
                (deployment_id,),
            )
            _write_last_commit(commit)
            return

        for fp in files_to_deploy:
            if not os.path.isfile(fp):
                print(f"  ⚠️ Skipping (not found): {fp}")
                continue

            sql = Path(fp).read_text()
            for var in ("SNOWFLAKE_DATABASE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_ENVIRONMENT"):
                sql = sql.replace(f"{{{{{var}}}}}", os.environ.get(var, ""))

            fhash = file_hash(fp)
            try:
                cur.execute(sql, num_statements=0)
                consume_results(cur)
                cur.execute("""
                    INSERT INTO TECH.DEPLOYMENT_FILE_HISTORY
                    (DEPLOYMENT_ID, FILE_PATH, FILE_HASH, STATUS, DEPLOYED_AT)
                    VALUES (%s, %s, %s, 'SUCCESS', CURRENT_TIMESTAMP())
                """, (deployment_id, fp, fhash))
                print(f"  ✓ {fp}")
            except Exception as e:
                err_msg = str(e)[:1000]
                cur.execute("""
                    INSERT INTO TECH.DEPLOYMENT_FILE_HISTORY
                    (DEPLOYMENT_ID, FILE_PATH, FILE_HASH, STATUS, ERROR_MESSAGE, DEPLOYED_AT)
                    VALUES (%s, %s, %s, 'FAILED', %s, CURRENT_TIMESTAMP())
                """, (deployment_id, fp, fhash, err_msg))
                cur.execute(
                    "UPDATE TECH.DEPLOYMENT_HISTORY SET DEPLOYMENT_STATUS = 'FAILED', END_TIME = CURRENT_TIMESTAMP() WHERE DEPLOYMENT_ID = %s",
                    (deployment_id,),
                )
                print(f"  ✗ {fp}: {e}", file=sys.stderr)
                sys.exit(1)

        cur.execute(
            "UPDATE TECH.DEPLOYMENT_HISTORY SET DEPLOYMENT_STATUS = 'SUCCESS', END_TIME = CURRENT_TIMESTAMP() WHERE DEPLOYMENT_ID = %s",
            (deployment_id,),
        )
        print("SQL executed successfully.")
    finally:
        conn.close()

    _write_last_commit(commit)
    print("✅ DDL deployment complete.")


def _dry_run(files: list[str]):
    """Preview files that would be deployed, with variable substitution."""
    print(f"🔍 DRY RUN — no changes will be made to Snowflake\n")
    print(f"Would deploy {len(files)} file(s):")
    db = os.environ.get("SNOWFLAKE_DATABASE", "<SNOWFLAKE_DATABASE>")
    wh = os.environ.get("SNOWFLAKE_WAREHOUSE", "<SNOWFLAKE_WAREHOUSE>")
    env = os.environ.get("SNOWFLAKE_ENVIRONMENT", "<SNOWFLAKE_ENVIRONMENT>")
    for fp in files:
        if not fp.endswith(".sql"):
            print(f"  ⚠️  {fp} (skipped: not .sql)")
            continue
        print(f"  📄 {fp}")
        if os.path.isfile(fp):
            content = Path(fp).read_text()
            content = content.replace("{{SNOWFLAKE_DATABASE}}", db)
            content = content.replace("{{SNOWFLAKE_WAREHOUSE}}", wh)
            content = content.replace("{{SNOWFLAKE_ENVIRONMENT}}", env)
            lines = content.splitlines()
            print("     Preview (with variable substitution):")
            for line in lines[:20]:
                print(f"     | {line}")
            if len(lines) > 20:
                print(f"     | ... ({len(lines)} lines total)")
    print(f"\n✅ Dry run complete. Run 'make deploy' to execute.")


def _write_last_commit(commit: str):
    """Save commit SHA for local change detection on next run."""
    Path(LAST_DEPLOY_FILE).write_text(commit + "\n")


def _git_branch() -> str:
    try:
        return git("branch", "--show-current")
    except Exception:
        return "local"


def _git_head() -> str:
    try:
        return git("rev-parse", "HEAD")
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="deploy.py",
        description="Snowflake DDL deployment CLI — detect changes, deploy, query history.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # detect-changes
    p_detect = sub.add_parser("detect-changes", help="Build file list (git diff or all .sql)")
    p_detect.add_argument("--mode", choices=["diff", "log"], default="diff",
                          help="diff = git diff (default), log = all .sql files")
    p_detect.add_argument("--base-ref", default=None,
                          help="Git ref to diff against (default: auto-detect, use 'snowflake' for CI)")

    # deploy
    p_deploy = sub.add_parser("deploy", help="Deploy SQL files to Snowflake")
    p_deploy.add_argument("--dry-run", action="store_true", help="Preview only, no execution")
    p_deploy.add_argument("--file-list", default=OUTPUT_FILE,
                          help=f"Path to file list (default: {OUTPUT_FILE})")
    p_deploy.add_argument("--branch", default=None, help="Branch name (auto-detected if omitted)")
    p_deploy.add_argument("--commit", default=None, help="Commit SHA (auto-detected if omitted)")

    # last-commit
    p_last = sub.add_parser("last-commit", help="Print last successful deploy commit SHA")
    p_last.add_argument("--branch", required=True, help="Branch to query")

    args = parser.parse_args()

    if args.command == "detect-changes":
        cmd_detect_changes(args)
    elif args.command == "deploy":
        cmd_deploy(args)
    elif args.command == "last-commit":
        cmd_last_commit(args)


if __name__ == "__main__":
    main()
