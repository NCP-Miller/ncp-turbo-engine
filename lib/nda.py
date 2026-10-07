"""NDA Review engine — applies NCP's NDA rulebook to uploaded NDAs.

The rulebook (nda_rules.md, seeded from the NCP NDA Review Handoff and
editable in the app) drives a GPT-4o review that buckets every issue the
NCP way — non-negotiable / credibility trade / accepted as drafted —
flags counsel escalations, and produces concrete edits.

Outputs per round:
  - CLEAN docx: NCP's position applied as plain text (this is exactly
    the "write NCP's position as clean text" artifact the locked
    LibreOffice-compare workflow consumes)
  - MARKED docx: a visual review copy (red strikethrough deletions,
    blue underline insertions). It is NOT Word tracked changes — the
    handoff's locked workflow owns true redline generation.

Projects persist across rounds in pipeline_data/nda_review.db, named by
deal project so iterations can be reviewed until the NDA is executable,
and mirror to the GitHub data branch.
"""

import io
import json
import os
import sqlite3
from datetime import datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DB_DIR = os.path.join(_ROOT, "pipeline_data")
_DB = os.path.join(_DB_DIR, "nda_review.db")
_RULES_FILE = os.path.join(_ROOT, "nda_rules.md")

DEAL_TYPES = [
    "Add-on for a portfolio company",
    "New platform — potentially competitive",
    "New platform — non-competitive",
]
NCP_ROLES = ["Recipient (NCP receiving)", "Mutual form", "Disclosing Party"]
FORM_SOURCES = ["Company-direct form", "Banker/agent form (sell-side)",
                "Unknown"]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _connect():
    os.makedirs(_DB_DIR, exist_ok=True)
    conn = sqlite3.connect(_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = _connect()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            deal_type TEXT, ncp_role TEXT, form_source TEXT,
            status TEXT DEFAULT 'in_review',
            created_at TEXT, updated_at TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            round_no INTEGER NOT NULL,
            filename TEXT, original_text TEXT,
            review_json TEXT, created_at TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS nda_meta (
            key TEXT PRIMARY KEY, value TEXT)""")
        cols = {r[1] for r in
                conn.execute("PRAGMA table_info(rounds)").fetchall()}
        if "file_blob" not in cols:
            # source .docx bytes, kept so the tracked-changes redline can
            # be regenerated against the counterparty's own formatting
            conn.execute("ALTER TABLE rounds ADD COLUMN file_blob BLOB")
        conn.commit()
    finally:
        conn.close()


# ── Rules ─────────────────────────────────────────────────────────────

def load_rules_file():
    """The repo's seed rulebook (nda_rules.md) — the latest handoff."""
    try:
        with open(_RULES_FILE) as f:
            return f.read()
    except FileNotFoundError:
        return ""


def load_rules():
    """User-edited rules from the DB win; fall back to nda_rules.md."""
    init_db()
    conn = _connect()
    try:
        r = conn.execute(
            "SELECT value FROM nda_meta WHERE key = 'rules'").fetchone()
        if r and (r["value"] or "").strip():
            return r["value"]
    finally:
        conn.close()
    return load_rules_file()


def save_rules(text):
    init_db()
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO nda_meta (key, value) VALUES ('rules', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (text,))
        conn.commit()
    finally:
        conn.close()


# ── Text extraction ───────────────────────────────────────────────────

def extract_text(file_bytes, filename):
    """Pull plain text from a .docx or .pdf upload."""
    name = filename.lower()
    if name.endswith(".docx"):
        import docx
        d = docx.Document(io.BytesIO(file_bytes))
        parts = [p.text for p in d.paragraphs]
        for t in d.tables:
            for row in t.rows:
                parts.append(" | ".join(c.text for c in row.cells))
        return "\n".join(parts)
    if name.endswith(".pdf"):
        try:
            from pypdf import PdfReader
        except Exception as e:
            raise RuntimeError(
                f"PDF support unavailable on this server ({e}). "
                f"Upload the .docx instead.")
        reader = PdfReader(io.BytesIO(file_bytes))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    raise RuntimeError("Upload a .docx or .pdf file.")


# ── AI review ─────────────────────────────────────────────────────────
# Claude (Fable, extended thinking) is the preferred negotiation brain;
# Opus/Sonnet are access fallbacks, GPT-4o the last resort.

ANTHROPIC_MODELS = ["claude-fable-5-1", "claude-opus-5", "claude-sonnet-5"]


def get_review_model():
    init_db()
    conn = _connect()
    try:
        r = conn.execute(
            "SELECT value FROM nda_meta WHERE key = 'model'").fetchone()
        return r["value"] if r and r["value"] in ANTHROPIC_MODELS \
            else ANTHROPIC_MODELS[0]
    finally:
        conn.close()


def set_review_model(model):
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO nda_meta (key, value) VALUES ('model', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (model,))
        conn.commit()
    finally:
        conn.close()


def _extract_json(text):
    import re
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        raise ValueError("The model returned no JSON.")
    return json.loads(m.group())


def review_nda(nda_text, deal_type, ncp_role, form_source, rules,
               prior_context="", anthropic_client=None,
               anthropic_model=None, openai_client=None):
    """One review round. Returns (review_dict, engine_label)."""
    prompt = f"""You are New Capital Partners' NDA reviewer. Apply the NCP NDA
rulebook below EXACTLY — it is the authority. Where the rulebook and
general practice disagree, the rulebook wins.

━━━━━━━━━ THE NCP NDA RULEBOOK ━━━━━━━━━
{rules[:24000]}
━━━━━━━━━ END RULEBOOK ━━━━━━━━━

THIS DEAL'S INTAKE GATES (already answered by Trey):
- Deal type: {deal_type}
  (ADD-ON FOR A PORTFOLIO COMPANY: competitive overlap EXISTS BY
   DEFINITION — the target operates in an NCP portfolio company's
   space. Apply the FULL protection set: portfolio company
   non-imputation, non-solicit carve-out for portfolio companies,
   Dual Hat Employees clause, hardened Competing Investments — AND
   include Affiliates in the Representatives definition per the
   ladder ("Include only on add-on deals").
   NEW PLATFORM, POTENTIALLY COMPETITIVE: apply the FULL protection
   set as well.
   NEW PLATFORM, NON-COMPETITIVE: standard protection set only; the
   standalone Competing Investments clause is still required.)
- NCP role: {ncp_role}
  (As Recipient, carve-outs stay broad — never apply Disclosing-Party
   moves. On a mutual form, identify the net discloser and say so.)
- Form source: {form_source}
  (Banker forms: hunt for standstills, broad no-contact lists, tails.)

{("PRIOR ROUNDS OF THIS NEGOTIATION:" + chr(10) + prior_context) if prior_context else "This is Round 1."}

THE NDA TEXT:
{nda_text[:60000]}

YOUR JOB:
1. Check every clause against the non-negotiables, standard terms, and
   concession ladder. Missing required clauses (standalone Competing
   Investments with survival/supremacy, lenders in Representatives,
   Dual Hat where competitive, no-obligation-to-proceed, etc.) must be
   ADDED as insertions.
2. Bucket every issue exactly as the rulebook does: "non_negotiable"
   (always hold), "trade" (credibility trades / ladder fallbacks —
   name the fallback), "accepted" (as good as or better than NCP
   standard — list them so nothing is silently accepted).
3. Flag counsel escalations (liquidated damages, indemnification,
   exclusivity, anything binding LPs) and the watch-list traps
   (standstills, broad CI definitions, non-solicits that work as
   non-competes, forum away from Delaware, tails).
4. Produce concrete text edits. For each: the EXACT original text to
   change (verbatim, long enough to be unique in the document; empty
   string for a pure insertion), the revised NCP text, where to insert
   if new (after which clause), the rationale citing the rulebook, and
   its bucket.
5. Fill NCP party details per the signing table (NCP Management
   Holdings, LLC; 2101 Highland Ave S, Suite 700, Birmingham, AL
   35205; William "Trey" Miller III, Managing Director and Authorized
   Signor; By: line left blank). Never leave NCP placeholders.
6. Judge executability: is this draft signable by both sides as-is
   after these edits, or does it still need the counterparty to move?

STYLE (from the rulebook): no em dashes or long dashes anywhere in
drafted text — commas, periods, or parentheses instead. Short, direct,
honest about weak spots. Explain direction of benefit where a
counterparty change actually helps NCP.

Return JSON only:
{{"summary": "3-5 sentence report to Trey",
  "executable": true/false,
  "executable_reasoning": "one sentence",
  "net_discloser_note": "for mutual forms, who is the net discloser and what that means; else empty",
  "escalations": ["counsel red-flag items found, with clause reference"],
  "accepted_as_drafted": ["provisions left alone but listed for Trey"],
  "edits": [{{"bucket": "non_negotiable|trade|style",
             "original": "exact text to replace, or empty for insertion",
             "insert_after": "anchor text when original is empty, else empty",
             "revised": "NCP's replacement/new text",
             "rationale": "why, citing the rulebook"}}]
}}"""

    # Preferred: Claude with extended thinking — real negotiation
    # reasoning before the redline, not just pattern matching.
    if anthropic_client is not None:
        first = anthropic_model or get_review_model()
        order = [first] + [m for m in ANTHROPIC_MODELS if m != first]
        last_err = None
        for model in order:
            for use_thinking in (True, False):
                try:
                    kwargs = {
                        "model": model,
                        "max_tokens": 16000,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                    if use_thinking:
                        kwargs["thinking"] = {"type": "enabled",
                                              "budget_tokens": 8000}
                    resp = anthropic_client.messages.create(**kwargs)
                    text = "".join(
                        b.text for b in resp.content
                        if getattr(b, "type", "") == "text")
                    label = (f"Claude {model}"
                             + (" · extended thinking" if use_thinking else ""))
                    return _extract_json(text), label
                except Exception as e:
                    last_err = e
                    continue
        if openai_client is None:
            raise RuntimeError(f"Claude review failed: {last_err}")
        _fallback_note = (f"GPT-4o (fallback — Claude failed: "
                          f"{type(last_err).__name__}: "
                          f"{str(last_err)[:300]})")
    else:
        _fallback_note = "GPT-4o (no ANTHROPIC_API_KEY configured)"

    if openai_client is None:
        raise RuntimeError(
            "No AI configured. Add ANTHROPIC_API_KEY (preferred — enables "
            "Claude with extended thinking) or OPENAI_API_KEY to secrets.")
    resp = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.2,
        timeout=120,
    )
    return json.loads(resp.choices[0].message.content), _fallback_note


# ── Document builders ─────────────────────────────────────────────────

def apply_edits(original_text, edits):
    """Apply edits to produce NCP's clean position text.

    Mirrors the handoff's uniqueness rule: a replacement only applies
    when its original text appears EXACTLY once; otherwise the edit is
    reported as unapplied instead of silently guessing (the silent-skip
    trap the handoff warns about). Returns (clean_text, applied, skipped).
    """
    text = original_text
    applied, skipped = [], []
    for e in edits:
        orig = (e.get("original") or "").strip()
        revised = e.get("revised") or ""
        if orig:
            count = text.count(orig)
            if count == 1:
                text = text.replace(orig, revised)
                applied.append(e)
            else:
                skipped.append({**e, "_skip_reason":
                                f"original text found {count} times "
                                f"(needs exactly 1)"})
        else:
            anchor = (e.get("insert_after") or "").strip()
            if anchor and text.count(anchor) == 1:
                idx = text.index(anchor) + len(anchor)
                text = text[:idx] + "\n\n" + revised + text[idx:]
                applied.append(e)
            else:
                # Fall back: append new clauses before any signature block
                text = text + "\n\n" + revised
                applied.append({**e, "_appended": True})
    return text, applied, skipped


def build_clean_docx(clean_text, project_name, round_no):
    """NCP's position as a plain .docx (the locked workflow's step-2 input)."""
    import docx
    d = docx.Document()
    d.add_heading(f"{project_name} — NCP Position (Round {round_no})", level=1)
    d.add_paragraph("Prepared by New Capital Partners. Clean text, "
                    "no tracked changes.")
    for para in clean_text.split("\n"):
        d.add_paragraph(para)
    buf = io.BytesIO()
    d.save(buf)
    return buf.getvalue()


def build_marked_docx(original_text, edits, project_name, round_no):
    """Visual review copy: deletions in red strikethrough, insertions in
    blue underline. Explicitly labeled — not Word tracked changes."""
    import docx
    from docx.shared import RGBColor
    RED, BLUE = RGBColor(0xC0, 0x00, 0x00), RGBColor(0x00, 0x56, 0xA7)

    by_para = {}
    insertions = []
    for e in edits:
        orig = (e.get("original") or "").strip()
        if orig:
            by_para.setdefault(orig.split("\n")[0], []).append(e)
        else:
            insertions.append(e)

    d = docx.Document()
    d.add_heading(f"{project_name} — Marked Review Copy (Round {round_no})",
                  level=1)
    d.add_paragraph(
        "Visual markup by New Capital Partners: red strikethrough = "
        "delete, blue underline = NCP insertion. This is a review copy, "
        "not a tracked-changes redline — generate the executable redline "
        "through the locked compare workflow.")

    for para in original_text.split("\n"):
        hits = [e for e in edits
                if (e.get("original") or "").strip()
                and (e["original"].strip().split("\n")[0] in para
                     or para.strip() and para.strip() in e["original"])]
        if not hits:
            d.add_paragraph(para)
            continue
        p = d.add_paragraph()
        consumed = False
        for e in hits:
            orig_first = e["original"].strip().split("\n")[0]
            if orig_first in para and not consumed:
                before, _, after = para.partition(orig_first)
                if before:
                    p.add_run(before)
                r_del = p.add_run(e["original"].strip())
                r_del.font.strike = True
                r_del.font.color.rgb = RED
                r_ins = p.add_run(" " + (e.get("revised") or ""))
                r_ins.font.underline = True
                r_ins.font.color.rgb = BLUE
                if after:
                    p.add_run(after)
                consumed = True
        if not consumed:
            p.add_run(para)

    if insertions:
        d.add_heading("NCP insertions (new clauses)", level=2)
        for e in insertions:
            p = d.add_paragraph()
            r = p.add_run(e.get("revised") or "")
            r.font.underline = True
            r.font.color.rgb = BLUE
            d.add_paragraph(f"Rationale: {e.get('rationale', '')}")

    buf = io.BytesIO()
    d.save(buf)
    return buf.getvalue()


# ── TRUE tracked-changes redline (w:ins / w:del OOXML) ───────────────
# Edits the counterparty's own .docx so their styles and numbering are
# preserved, injecting genuine Word tracked changes per the handoff:
# author "New Capital Partners", change IDs from 960, every substitution
# uniqueness-checked, and the accept/reject audits (the authoritative
# correctness tests) run on every build.

TRACK_AUTHOR = "New Capital Partners"
TRACK_ID_START = 960


def _iter_paragraphs(doc):
    """Every paragraph in document order, including inside tables."""
    from docx.text.paragraph import Paragraph
    from docx.oxml.ns import qn
    for p in doc.element.body.iter(qn("w:p")):
        yield Paragraph(p, doc)


def _walker_text(doc):
    return "\n".join(p.text for p in _iter_paragraphs(doc))


def _defragment(doc):
    """Merge each paragraph's runs into one (Word splits words across
    tiny runs; without this, text searches fail silently — the exact
    trap the handoff warns about)."""
    for para in _iter_paragraphs(doc):
        runs = para.runs
        if len(runs) > 1:
            full = "".join(r.text for r in runs)
            runs[0].text = full
            for r in runs[1:]:
                r._r.getparent().remove(r._r)


def _xml_escape(t):
    return (str(t).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;"))


def build_tracked_docx(original_docx_bytes, edits):
    """Inject genuine tracked changes into the uploaded .docx.

    Returns (docx_bytes, report) where report carries applied/skipped
    edits and the accept/reject audit results.
    """
    import docx
    from docx.oxml import parse_xml
    from docx.oxml.ns import nsdecls, qn

    doc = docx.Document(io.BytesIO(original_docx_bytes))
    _defragment(doc)
    base_text = _walker_text(doc)
    date = _now()[:19] + "Z"
    next_id = [TRACK_ID_START]

    def _rid():
        next_id[0] += 1
        return next_id[0] - 1

    def _rpr_xml(para):
        if para.runs and para.runs[0]._r.rPr is not None:
            return para.runs[0]._r.rPr.xml
        return ""

    def _plain_run(text, rpr):
        return parse_xml(
            f'<w:r {nsdecls("w")}>{rpr}'
            f'<w:t xml:space="preserve">{_xml_escape(text)}</w:t></w:r>')

    def _del_el(text, rpr):
        return parse_xml(
            f'<w:del {nsdecls("w")} w:id="{_rid()}" '
            f'w:author="{TRACK_AUTHOR}" w:date="{date}"><w:r>{rpr}'
            f'<w:delText xml:space="preserve">{_xml_escape(text)}'
            f'</w:delText></w:r></w:del>')

    def _ins_el(text, rpr):
        return parse_xml(
            f'<w:ins {nsdecls("w")} w:id="{_rid()}" '
            f'w:author="{TRACK_AUTHOR}" w:date="{date}"><w:r>{rpr}'
            f'<w:t xml:space="preserve">{_xml_escape(text)}</w:t>'
            f'</w:r></w:ins>')

    def _replace_in_para(para, orig, revised):
        full = para.text
        before, _, after = full.partition(orig)
        rpr = _rpr_xml(para)
        for r in list(para.runs):
            r._r.getparent().remove(r._r)
        if before:
            para._p.append(_plain_run(before, rpr))
        para._p.append(_del_el(orig, rpr))
        if revised:
            para._p.append(_ins_el(revised, rpr))
        if after:
            para._p.append(_plain_run(after, rpr))

    applied, skipped = [], []
    # Parallel text simulation, line-aligned with the paragraph DOM, so
    # the accept-audit has an independent expectation that reflects
    # WHERE insertions land, not just that they exist.
    sim_lines = base_text.split("\n")

    for e in edits:
        orig = (e.get("original") or "").strip()
        revised = e.get("revised") or ""
        if not orig:
            continue                     # insertions handled below
        paras = list(_iter_paragraphs(doc))
        if base_text.count(orig) == 1 and "\n" not in orig:
            idx = next((i for i, p in enumerate(paras) if orig in p.text),
                       None)
            if idx is None:
                skipped.append({**e, "_skip_reason":
                                "text crosses a paragraph boundary"})
                continue
            full = paras[idx].text
            _replace_in_para(paras[idx], orig, revised)
            sim_lines[idx] = full.replace(orig, revised)
            applied.append(e)
        elif "\n" in orig:
            # multi-paragraph original: match a consecutive para sequence
            orig_lines = [l for l in orig.split("\n")]
            texts = [p.text for p in paras]
            hit = None
            for i in range(len(texts) - len(orig_lines) + 1):
                if [t.strip() for t in texts[i:i + len(orig_lines)]] == \
                        [l.strip() for l in orig_lines]:
                    hit = i
                    break
            if hit is None:
                skipped.append({**e, "_skip_reason":
                                "multi-paragraph text not matched"})
                continue
            _replace_in_para(paras[hit], texts[hit], revised)
            sim_lines[hit] = revised
            for j in range(1, len(orig_lines)):
                _replace_in_para(paras[hit + j], texts[hit + j], "")
                sim_lines[hit + j] = ""
            applied.append(e)
        else:
            n = base_text.count(orig)
            skipped.append({**e, "_skip_reason":
                            f"original text found {n} times "
                            f"(needs exactly 1)"})

    # Insertions: new clauses as fully-inserted paragraphs
    for e in edits:
        if (e.get("original") or "").strip():
            continue
        revised = e.get("revised") or ""
        if not revised.strip():
            continue
        anchor_text = (e.get("insert_after") or "").strip()
        paras = list(_iter_paragraphs(doc))
        k = next((i for i, p in enumerate(paras)
                  if anchor_text and anchor_text in p.text), None)
        new_p = parse_xml(f'<w:p {nsdecls("w")}></w:p>')
        new_p.append(_ins_el(revised, ""))
        if k is not None:
            paras[k]._p.addnext(new_p)
            sim_lines.insert(k + 1, revised)
        else:
            doc.element.body.insert(len(doc.element.body) - 1, new_p)
            sim_lines.append(revised)
        applied.append(e)

    expected_clean = "\n".join(sim_lines)

    buf = io.BytesIO()
    doc.save(buf)
    out = buf.getvalue()

    # ── The authoritative audits (handoff: screen appearance is not) ──
    def _norm(s):
        return "\n".join(l.strip() for l in s.split("\n") if l.strip())

    rdoc = docx.Document(io.BytesIO(out))
    acc_lines, rej_lines = [], []
    for para in _iter_paragraphs(rdoc):
        acc, rej = "", ""
        for child in para._p:
            if child.tag == qn("w:r"):
                t = "".join(n.text or "" for n in child.iter(qn("w:t")))
                acc += t
                rej += t
            elif child.tag == qn("w:ins"):
                acc += "".join(n.text or "" for n in child.iter(qn("w:t")))
            elif child.tag == qn("w:del"):
                rej += "".join(n.text or ""
                               for n in child.iter(qn("w:delText")))
        acc_lines.append(acc)
        rej_lines.append(rej)

    accept_audit = _norm("\n".join(acc_lines)) == _norm(expected_clean)
    reject_audit = _norm("\n".join(rej_lines)) == _norm(base_text)

    report = {
        "applied": len(applied),
        "skipped": skipped,
        "accept_audit": accept_audit,
        "reject_audit": reject_audit,
        "author": TRACK_AUTHOR,
        "first_id": TRACK_ID_START,
    }
    return out, report


def count_tracked_changes(docx_bytes):
    """Handoff incoming-file check: how many live w:ins / w:del marks?"""
    import zipfile as _zf
    try:
        with _zf.ZipFile(io.BytesIO(docx_bytes)) as z:
            xml = z.read("word/document.xml").decode("utf-8", "replace")
        return xml.count("<w:ins "), xml.count("<w:del ")
    except Exception:
        return 0, 0


# ── Project / round storage ──────────────────────────────────────────

def create_project(name, deal_type, ncp_role, form_source):
    init_db()
    conn = _connect()
    try:
        existing = conn.execute(
            "SELECT id FROM projects WHERE name = ?", (name,)).fetchone()
        if existing:
            return existing["id"]
        cur = conn.execute(
            """INSERT INTO projects (name, deal_type, ncp_role, form_source,
               created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)""",
            (name, deal_type, ncp_role, form_source, _now(), _now()))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def list_projects():
    init_db()
    conn = _connect()
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM projects ORDER BY updated_at DESC").fetchall()]
    finally:
        conn.close()


def set_project_status(project_id, status):
    conn = _connect()
    try:
        conn.execute("UPDATE projects SET status = ?, updated_at = ? "
                     "WHERE id = ?", (status, _now(), project_id))
        conn.commit()
    finally:
        conn.close()


def add_round(project_id, filename, original_text, review, file_blob=None):
    conn = _connect()
    try:
        prev = conn.execute(
            "SELECT MAX(round_no) AS n FROM rounds WHERE project_id = ?",
            (project_id,)).fetchone()
        round_no = (prev["n"] or 0) + 1
        conn.execute(
            """INSERT INTO rounds (project_id, round_no, filename,
               original_text, review_json, created_at, file_blob)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (project_id, round_no, filename, original_text,
             json.dumps(review, default=str), _now(), file_blob))
        conn.execute("UPDATE projects SET updated_at = ? WHERE id = ?",
                     (_now(), project_id))
        conn.commit()
        return round_no
    finally:
        conn.close()


def get_round_blob(round_id):
    conn = _connect()
    try:
        r = conn.execute("SELECT file_blob FROM rounds WHERE id = ?",
                         (round_id,)).fetchone()
        return r["file_blob"] if r else None
    finally:
        conn.close()


def list_rounds(project_id):
    conn = _connect()
    try:
        rows = conn.execute(
            """SELECT id, project_id, round_no, filename, original_text,
                      review_json, created_at,
                      (file_blob IS NOT NULL) AS has_blob
               FROM rounds WHERE project_id = ? ORDER BY round_no""",
            (project_id,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["review"] = json.loads(d.pop("review_json") or "{}")
            except json.JSONDecodeError:
                d["review"] = {}
            out.append(d)
        return out
    finally:
        conn.close()


def prior_context(project_id):
    """Compressed history of earlier rounds for the next review prompt."""
    lines = []
    for r in list_rounds(project_id):
        rv = r.get("review", {})
        lines.append(f"Round {r['round_no']} ({(r['created_at'] or '')[:10]}): "
                     f"{rv.get('summary', 'no summary')} "
                     f"Executable: {rv.get('executable')}")
    return "\n".join(lines)


# ── Backup ────────────────────────────────────────────────────────────

def export_all():
    init_db()
    conn = _connect()
    try:
        return {
            "projects": [dict(r) for r in conn.execute(
                "SELECT * FROM projects").fetchall()],
            # file_blob excluded: binary stays local; text + edits are
            # enough to regenerate clean/marked copies after a restore
            "rounds": [dict(r) for r in conn.execute(
                """SELECT id, project_id, round_no, filename,
                   original_text, review_json, created_at
                   FROM rounds""").fetchall()],
            "meta": [dict(r) for r in conn.execute(
                "SELECT * FROM nda_meta").fetchall()],
        }
    finally:
        conn.close()


def backup_to_github():
    try:
        from lib.github_backup import backup_nda
        return backup_nda(export_all())
    except Exception:
        return False


def restore_if_empty():
    init_db()
    conn = _connect()
    try:
        n = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    finally:
        conn.close()
    if n:
        return False
    try:
        from lib.github_backup import restore_nda
        data = restore_nda()
        if not data:
            return False
        conn = _connect()
        try:
            for p in data.get("projects", []):
                cols = list(p.keys())
                conn.execute(
                    f"INSERT OR IGNORE INTO projects ({','.join(cols)}) "
                    f"VALUES ({','.join('?' * len(cols))})",
                    [p[c] for c in cols])
            for r in data.get("rounds", []):
                cols = list(r.keys())
                conn.execute(
                    f"INSERT OR IGNORE INTO rounds ({','.join(cols)}) "
                    f"VALUES ({','.join('?' * len(cols))})",
                    [r[c] for c in cols])
            for m in data.get("meta", []):
                conn.execute(
                    "INSERT OR IGNORE INTO nda_meta (key, value) VALUES (?, ?)",
                    (m.get("key"), m.get("value")))
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception:
        return False
