import csv
import io
import os
import sqlite3
from datetime import datetime, timedelta

from flask import Flask, jsonify, request, send_file, send_from_directory
from flask_cors import CORS

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)

DB_PATH = os.environ.get("DB_PATH", "/data/pricedesk.db")

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db():
    db_dir = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _get_schema_version(conn):
    try:
        row = conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        return row[0] if row and row[0] is not None else 0
    except sqlite3.OperationalError:
        return 0


def _migrate_to_v1(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS products (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT    NOT NULL,
            code           TEXT    UNIQUE,
            category       TEXT,
            unit           TEXT,
            price          REAL    NOT NULL,
            cost           REAL,
            notes          TEXT,
            stock_on_hand  REAL    NOT NULL DEFAULT 0,
            reorder_level  REAL,
            created_at     TEXT    NOT NULL,
            updated_at     TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sale_events (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            sold_at        TEXT    NOT NULL,
            notes          TEXT,
            total_revenue  REAL    NOT NULL DEFAULT 0,
            total_cost     REAL    NOT NULL DEFAULT 0,
            total_profit   REAL    NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS sale_items (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id       INTEGER NOT NULL REFERENCES sale_events(id) ON DELETE CASCADE,
            product_id    INTEGER REFERENCES products(id) ON DELETE SET NULL,
            product_name  TEXT    NOT NULL,
            quantity      REAL    NOT NULL,
            unit_price    REAL    NOT NULL,
            unit_cost     REAL    NOT NULL DEFAULT 0,
            line_total    REAL    NOT NULL,
            line_profit   REAL    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stock_adjustments (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id   INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            quantity     REAL    NOT NULL,
            reason       TEXT,
            adjusted_at  TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_products_name
            ON products(name);
        CREATE INDEX IF NOT EXISTS idx_products_category
            ON products(category);
        CREATE INDEX IF NOT EXISTS idx_sale_events_sold_at
            ON sale_events(sold_at);
        CREATE INDEX IF NOT EXISTS idx_sale_items_sale_id
            ON sale_items(sale_id);
        CREATE INDEX IF NOT EXISTS idx_sale_items_product_id
            ON sale_items(product_id);

        INSERT INTO schema_version VALUES (1);
        """
    )


def _run_migrations(conn):
    current = _get_schema_version(conn)
    if current < 1:
        _migrate_to_v1(conn)


def init_db():
    """Run at import time so gunicorn workers initialise the DB."""
    conn = get_db()
    try:
        _run_migrations(conn)
    finally:
        conn.close()


# Runs once per worker process when gunicorn imports this module.
init_db()

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(v):
    try:
        return float(v) if v is not None and str(v).strip() != "" else None
    except (ValueError, TypeError):
        return None


def _parse_float(v, name):
    """Return (float_value, None) or (None, error_message) — no exceptions raised."""
    if v is None or str(v).strip() == "":
        return None, f"{name} is required"
    try:
        return float(v), None
    except (ValueError, TypeError):
        return None, f"{name} must be a number"


def _validate_product_data(data):
    """Validate + extract product fields.

    Returns (fields_dict, None) on success or (None, error_message) on failure.
    No exceptions are raised so validation errors are never tainted by stack info.
    """
    name = (data.get("name") or "").strip()
    if not name:
        return None, "name is required"
    price, err = _parse_float(data.get("price"), "price")
    if err:
        return None, err
    return dict(
        name=name,
        code=data.get("code") or None,
        category=data.get("category") or None,
        unit=data.get("unit") or None,
        price=price,
        cost=_safe_float(data.get("cost")),
        notes=data.get("notes") or None,
        stock_on_hand=_safe_float(data.get("stock_on_hand")) or 0.0,
        reorder_level=_safe_float(data.get("reorder_level")),
    ), None


# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/health")
def health():
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

@app.route("/api/products", methods=["GET"])
def list_products():
    q = request.args.get("q", "").strip()
    category = request.args.get("category", "").strip()
    conn = get_db()
    try:
        sql = "SELECT * FROM products WHERE 1=1"
        params = []
        if q:
            sql += " AND (name LIKE ? OR code LIKE ?)"
            params += [f"%{q}%", f"%{q}%"]
        if category:
            sql += " AND category = ?"
            params.append(category)
        sql += " ORDER BY name COLLATE NOCASE"
        rows = conn.execute(sql, params).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/products/categories", methods=["GET"])
def list_categories():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT DISTINCT category FROM products"
            " WHERE category IS NOT NULL ORDER BY category COLLATE NOCASE"
        ).fetchall()
        return jsonify([r[0] for r in rows])
    finally:
        conn.close()


@app.route("/api/products", methods=["POST"])
def create_product():
    fields, err = _validate_product_data(request.get_json(force=True) or {})
    if err:
        return jsonify({"error": err}), 400
    now = _now()
    conn = get_db()
    try:
        cur = conn.execute(
            """INSERT INTO products
               (name, code, category, unit, price, cost, notes,
                stock_on_hand, reorder_level, created_at, updated_at)
               VALUES (:name,:code,:category,:unit,:price,:cost,:notes,
                       :stock_on_hand,:reorder_level,:now,:now)""",
            {**fields, "now": now},
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM products WHERE id=?", (cur.lastrowid,)
        ).fetchone()
        return jsonify(dict(row)), 201
    except sqlite3.IntegrityError as e:
        return jsonify({"error": "Product code already exists" if "UNIQUE" in str(e) else "A database error occurred. Check that the product code is unique."}), 409
    finally:
        conn.close()


@app.route("/api/products/<int:pid>", methods=["GET"])
def get_product(pid):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        return jsonify(dict(row))
    finally:
        conn.close()


@app.route("/api/products/<int:pid>", methods=["PUT"])
def update_product(pid):
    fields, err = _validate_product_data(request.get_json(force=True) or {})
    if err:
        return jsonify({"error": err}), 400
    now = _now()
    conn = get_db()
    try:
        conn.execute(
            """UPDATE products SET
               name=:name, code=:code, category=:category, unit=:unit,
               price=:price, cost=:cost, notes=:notes,
               stock_on_hand=:stock_on_hand, reorder_level=:reorder_level,
               updated_at=:now
               WHERE id=:id""",
            {**fields, "now": now, "id": pid},
        )
        conn.commit()
        row = conn.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        return jsonify(dict(row))
    except sqlite3.IntegrityError as e:
        return jsonify({"error": "Product code already exists" if "UNIQUE" in str(e) else "A database error occurred. Check that the product code is unique."}), 409
    finally:
        conn.close()


@app.route("/api/products/<int:pid>", methods=["DELETE"])
def delete_product(pid):
    conn = get_db()
    try:
        conn.execute("DELETE FROM products WHERE id=?", (pid,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@app.route("/api/products/<int:pid>/adjust-stock", methods=["POST"])
def adjust_stock(pid):
    data = request.get_json(force=True) or {}
    qty, err = _parse_float(data.get("quantity"), "quantity")
    if err:
        return jsonify({"error": err}), 400
    reason = (data.get("reason") or "").strip()
    now = _now()
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        new_stock = row["stock_on_hand"] + qty
        conn.execute(
            "UPDATE products SET stock_on_hand=?, updated_at=? WHERE id=?",
            (new_stock, now, pid),
        )
        conn.execute(
            "INSERT INTO stock_adjustments (product_id, quantity, reason, adjusted_at)"
            " VALUES (?,?,?,?)",
            (pid, qty, reason or None, now),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
        return jsonify(dict(row))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------

@app.route("/api/sales", methods=["GET"])
def list_sales():
    limit = min(int(request.args.get("limit", 100)), 500)
    conn = get_db()
    try:
        events = conn.execute(
            "SELECT * FROM sale_events ORDER BY sold_at DESC LIMIT ?", (limit,)
        ).fetchall()
        result = []
        for ev in events:
            items = conn.execute(
                "SELECT * FROM sale_items WHERE sale_id=?", (ev["id"],)
            ).fetchall()
            result.append({**dict(ev), "items": [dict(i) for i in items]})
        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/sales", methods=["POST"])
def create_sale():
    data = request.get_json(force=True) or {}
    items = data.get("items") or []
    if not items:
        return jsonify({"error": "items is required and must not be empty"}), 400

    notes = (data.get("notes") or "").strip()
    sold_at = (data.get("sold_at") or "").strip() or _now()

    conn = get_db()
    try:
        total_revenue = 0.0
        total_cost = 0.0
        processed = []

        for item in items:
            product = None
            pid = item.get("product_id")
            if pid:
                product = conn.execute(
                    "SELECT * FROM products WHERE id=?", (pid,)
                ).fetchone()

            qty, err = _parse_float(item.get("quantity", 1), "quantity")
            if err:
                return jsonify({"error": err}), 400

            unit_price = _safe_float(item.get("unit_price"))
            if unit_price is None:
                unit_price = float(product["price"]) if product else 0.0

            unit_cost = _safe_float(item.get("unit_cost"))
            if unit_cost is None:
                unit_cost = float(product["cost"]) if product and product["cost"] else 0.0

            product_name = (item.get("product_name") or "").strip() or (
                product["name"] if product else "Unknown"
            )

            line_total = unit_price * qty
            line_profit = (unit_price - unit_cost) * qty
            total_revenue += line_total
            total_cost += unit_cost * qty

            processed.append(
                dict(
                    product_id=pid,
                    product_name=product_name,
                    quantity=qty,
                    unit_price=unit_price,
                    unit_cost=unit_cost,
                    line_total=line_total,
                    line_profit=line_profit,
                )
            )

        total_profit = total_revenue - total_cost

        cur = conn.execute(
            "INSERT INTO sale_events (sold_at, notes, total_revenue, total_cost, total_profit)"
            " VALUES (?,?,?,?,?)",
            (sold_at, notes or None, total_revenue, total_cost, total_profit),
        )
        sale_id = cur.lastrowid

        for item in processed:
            conn.execute(
                """INSERT INTO sale_items
                   (sale_id, product_id, product_name, quantity,
                    unit_price, unit_cost, line_total, line_profit)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    sale_id,
                    item["product_id"],
                    item["product_name"],
                    item["quantity"],
                    item["unit_price"],
                    item["unit_cost"],
                    item["line_total"],
                    item["line_profit"],
                ),
            )
            if item["product_id"]:
                conn.execute(
                    "UPDATE products SET stock_on_hand = stock_on_hand - ?, updated_at=?"
                    " WHERE id=?",
                    (item["quantity"], sold_at, item["product_id"]),
                )

        conn.commit()

        ev = conn.execute(
            "SELECT * FROM sale_events WHERE id=?", (sale_id,)
        ).fetchone()
        sale_items = conn.execute(
            "SELECT * FROM sale_items WHERE sale_id=?", (sale_id,)
        ).fetchall()
        return jsonify({**dict(ev), "items": [dict(i) for i in sale_items]}), 201
    finally:
        conn.close()


@app.route("/api/sales/<int:sid>", methods=["GET"])
def get_sale(sid):
    conn = get_db()
    try:
        ev = conn.execute(
            "SELECT * FROM sale_events WHERE id=?", (sid,)
        ).fetchone()
        if not ev:
            return jsonify({"error": "Not found"}), 404
        items = conn.execute(
            "SELECT * FROM sale_items WHERE sale_id=?", (sid,)
        ).fetchall()
        return jsonify({**dict(ev), "items": [dict(i) for i in items]})
    finally:
        conn.close()


@app.route("/api/sales/<int:sid>", methods=["DELETE"])
def delete_sale(sid):
    now = _now()
    conn = get_db()
    try:
        ev = conn.execute(
            "SELECT * FROM sale_events WHERE id=?", (sid,)
        ).fetchone()
        if not ev:
            return jsonify({"error": "Not found"}), 404

        items = conn.execute(
            "SELECT * FROM sale_items WHERE sale_id=?", (sid,)
        ).fetchall()
        for item in items:
            if item["product_id"]:
                conn.execute(
                    "UPDATE products SET stock_on_hand = stock_on_hand + ?, updated_at=?"
                    " WHERE id=?",
                    (item["quantity"], now, item["product_id"]),
                )

        conn.execute("DELETE FROM sale_events WHERE id=?", (sid,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

@app.route("/api/analytics", methods=["GET"])
def analytics():
    period = request.args.get("period", "today")
    now = datetime.utcnow()

    if period == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    elif period == "week":
        start = (now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif period == "month":
        start = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        start = "2000-01-01T00:00:00Z"

    conn = get_db()
    try:
        totals = conn.execute(
            """SELECT
                 COALESCE(SUM(total_revenue), 0) AS revenue,
                 COALESCE(SUM(total_cost),    0) AS cost,
                 COALESCE(SUM(total_profit),  0) AS profit,
                 COUNT(*)                         AS num_sales
               FROM sale_events WHERE sold_at >= ?""",
            (start,),
        ).fetchone()

        items_count = conn.execute(
            """SELECT COALESCE(SUM(si.quantity), 0) AS items_sold
               FROM sale_items si
               JOIN sale_events se ON se.id = si.sale_id
               WHERE se.sold_at >= ?""",
            (start,),
        ).fetchone()

        top_products = conn.execute(
            """SELECT si.product_name,
                      SUM(si.quantity)    AS qty_sold,
                      SUM(si.line_total)  AS revenue,
                      SUM(si.line_profit) AS profit
               FROM sale_items si
               JOIN sale_events se ON se.id = si.sale_id
               WHERE se.sold_at >= ?
               GROUP BY si.product_name
               ORDER BY revenue DESC
               LIMIT 10""",
            (start,),
        ).fetchall()

        timeline = conn.execute(
            """SELECT substr(sold_at, 1, 10) AS date,
                      SUM(total_revenue) AS revenue,
                      SUM(total_profit)  AS profit,
                      COUNT(*)           AS num_sales
               FROM sale_events WHERE sold_at >= ?
               GROUP BY date
               ORDER BY date""",
            (start,),
        ).fetchall()

        revenue = totals["revenue"]
        profit = totals["profit"]
        margin = round(profit / revenue * 100, 1) if revenue else 0

        return jsonify(
            {
                "period": period,
                "start": start,
                "revenue": revenue,
                "cost": totals["cost"],
                "profit": profit,
                "margin_pct": margin,
                "num_sales": totals["num_sales"],
                "items_sold": items_count["items_sold"],
                "top_products": [dict(r) for r in top_products],
                "timeline": [dict(r) for r in timeline],
            }
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Import / Export
# ---------------------------------------------------------------------------

REQUIRED_IMPORT_COLS = {"name", "price"}
ALL_IMPORT_COLS = {
    "name", "price", "cost", "category", "unit", "code", "notes",
    "stock_on_hand", "reorder_level",
}


@app.route("/api/import/products", methods=["POST"])
def import_products():
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file uploaded"}), 400

    try:
        content = f.read().decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError:
        return jsonify({"error": "File must be UTF-8 encoded. In Excel: Save As → CSV UTF-8. In Notepad: Save As → Encoding: UTF-8."}), 400
    reader = csv.DictReader(io.StringIO(content))
    fieldnames = set(reader.fieldnames or [])

    if not REQUIRED_IMPORT_COLS.issubset(fieldnames):
        missing = REQUIRED_IMPORT_COLS - fieldnames
        return jsonify({"error": f"CSV missing required columns: {sorted(missing)}"}), 400

    now = _now()
    conn = get_db()
    imported = 0
    updated = 0
    errors = []

    try:
        for lineno, row in enumerate(reader, start=2):
            name = (row.get("name") or "").strip()
            price_str = (row.get("price") or "").strip()
            if not name or not price_str:
                errors.append(f"Row {lineno}: name and price are required")
                continue
            try:
                price = float(price_str)
            except ValueError:
                errors.append(f"Row {lineno}: invalid price '{price_str}'")
                continue

            code = (row.get("code") or "").strip() or None

            try:
                if code:
                    # Upsert on code
                    existing = conn.execute(
                        "SELECT id FROM products WHERE code=?", (code,)
                    ).fetchone()
                    if existing:
                        conn.execute(
                            """UPDATE products SET
                               name=?, price=?, cost=?, category=?,
                               unit=?, notes=?, stock_on_hand=?,
                               reorder_level=?, updated_at=?
                               WHERE code=?""",
                            (
                                name,
                                price,
                                _safe_float(row.get("cost")),
                                row.get("category") or None,
                                row.get("unit") or None,
                                row.get("notes") or None,
                                _safe_float(row.get("stock_on_hand")) or 0.0,
                                _safe_float(row.get("reorder_level")),
                                now,
                                code,
                            ),
                        )
                        updated += 1
                        continue

                conn.execute(
                    """INSERT INTO products
                       (name, code, category, unit, price, cost, notes,
                        stock_on_hand, reorder_level, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        name,
                        code,
                        row.get("category") or None,
                        row.get("unit") or None,
                        price,
                        _safe_float(row.get("cost")),
                        row.get("notes") or None,
                        _safe_float(row.get("stock_on_hand")) or 0.0,
                        _safe_float(row.get("reorder_level")),
                        now,
                        now,
                    ),
                )
                imported += 1
            except sqlite3.IntegrityError as e:
                errors.append(f"Row {lineno}: {e}")

        conn.commit()
        return jsonify({"imported": imported, "updated": updated, "errors": errors})
    finally:
        conn.close()


@app.route("/api/export/products", methods=["GET"])
def export_products():
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM products ORDER BY name COLLATE NOCASE").fetchall()
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(
            ["name", "price", "cost", "category", "unit", "code", "notes",
             "stock_on_hand", "reorder_level"]
        )
        for r in rows:
            writer.writerow(
                [
                    r["name"],
                    r["price"],
                    r["cost"] if r["cost"] is not None else "",
                    r["category"] or "",
                    r["unit"] or "",
                    r["code"] or "",
                    r["notes"] or "",
                    r["stock_on_hand"],
                    r["reorder_level"] if r["reorder_level"] is not None else "",
                ]
            )
        out.seek(0)
        return send_file(
            io.BytesIO(out.read().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="products.csv",
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Dev entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
