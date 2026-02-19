# Run this script ONCE to:
# 1. Show all users currently in the database
# 2. Delete any unverified / stuck registrations (keeps admin)
#
# Usage: python cleanup_users.py

import os
from dotenv import load_dotenv
load_dotenv()

from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD'))}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

with engine.begin() as conn:
    # Show current users
    rows = conn.execute(text(
        "SELECT id, username, email, is_verified, role, created_at FROM users ORDER BY created_at DESC"
    )).fetchall()

    print("\n=== CURRENT USERS IN DATABASE ===")
    if not rows:
        print("  (no users found)")
    for r in rows:
        print(f"  id={r[0]}  username='{r[1]}'  email='{r[2]}'  verified={r[3]}  role={r[4]}  created={r[5]}")

    # Delete unverified non-admin users (stuck failed signups)
    deleted = conn.execute(text(
        "DELETE FROM users WHERE is_verified = FALSE AND role != 'admin' RETURNING username, email"
    )).fetchall()

    if deleted:
        print(f"\n=== CLEANED UP {len(deleted)} STUCK UNVERIFIED RECORD(S) ===")
        for d in deleted:
            print(f"  Deleted: username='{d[0]}'  email='{d[1]}'")
    else:
        print("\n=== No stuck records found (nothing to clean) ===")

    # Show remaining users
    remaining = conn.execute(text(
        "SELECT id, username, email, is_verified, role FROM users ORDER BY id"
    )).fetchall()
    print("\n=== REMAINING USERS ===")
    for r in remaining:
        print(f"  id={r[0]}  username='{r[1]}'  email='{r[2]}'  verified={r[3]}  role={r[4]}")

print("\nDone. You can now re-register with the same email.")
