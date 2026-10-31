"""Word tracked-changes engine for NDA Review.

Implements NCP's locked redline workflow natively (Streamlit Cloud has no
LibreOffice):

  1. Baseline: the counterparty's file with any tracked changes they sent
     accepted, so NCP's markup is a single layer.
  2. NCP's position: revised text for the paragraphs NCP changes, plus any
     new clauses, keyed to baseline paragraph numbers.
  3. Redline by comparison: each changed paragraph is diffed word by word
     against the baseline and the differences are written as w:ins / w:del
     inside the counterparty's own runs, so their formatting, numbering,
     hyperlinks and layout survive. Paragraphs NCP does not change are left
     exactly as the counterparty sent them.
  4. Audit before delivery: reject-all must reproduce the baseline,
     accept-all must equal NCP's position, untouched paragraphs must be
     unchanged, and the revision markup must be well formed, authored
     "New Capital Partners", with IDs from 960.

The CLEAN deliverable is the redline with every change accepted.
"""

import copy
import difflib
import io
import re
from datetime import datetime, timezone

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
W = "{%s}" % W_NS
MC = "{http://schemas.openxmlformats.org/markup-compatibility/2006}"
XML_SPACE = "{http://www.w3.org/XML/1998/namespace}space"

AUTHOR = "New Capital Partners"
ID_START = 960
# Shown to the reviewer in place of a line break inside a paragraph, so
# each paragraph stays on one line of the document view.
LINE_BREAK_MARK = "⏎"

_CONTAINERS = {W + t for t in ("hyperlink", "smartTag", "customXml", "sdt",
                               "sdtContent", "fldSimple", "dir", "bdo")}
_INS = {W + "ins", W + "moveTo"}
_DEL = {W + "del", W + "moveFrom"}
_MARKER_PARENTS = {W + "rPr", W + "trPr", W + "numPr", W + "tcPr"}
_PROPERTY_CHANGES = ("rPrChange", "pPrChange", "sectPrChange", "tblPrChange",
                     "tblGridChange", "trPrChange", "tcPrChange",
                     "tblPrExChange", "numberingChange")
_RANGE_MARKS = ("moveFromRangeStart", "moveFromRangeEnd", "moveToRangeStart",
                "moveToRangeEnd", "customXmlInsRangeStart",
                "customXmlInsRangeEnd", "customXmlDelRangeStart",
                "customXmlDelRangeEnd")
_STORY_PART = re.compile(r"^/word/(header|footer)\d*\.xml$|"
                         r"^/word/(footnotes|endnotes|comments)\.xml$")
SIGNATURE_CUE = re.compile(
    r"^\s*(in witness whereof|dated\b|date:|by:|by\s*_|name:|title:|"
    r"signature|authorized signature|accepted and agreed|agreed and accepted|"
    r"acknowledged and agreed|agreed to and accepted|very truly yours|"
    r"sincerely|\[signature)", re.I)


# ── Package helpers ──────────────────────────────────────────────────

def _load(docx_bytes):
    import docx
    return docx.Document(io.BytesIO(docx_bytes))


def _save(doc):
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


def _el(tag):
    from docx.oxml import OxmlElement
    return OxmlElement("w:" + tag)


def _is_tag(el):
    return isinstance(el.tag, str)


def _unwrap(el):
    parent = el.getparent()
    idx = parent.index(el)
    for child in list(el):
        parent.insert(idx, child)
        idx += 1
    parent.remove(el)


def _in_textbox(p):
    a = p.getparent()
    while a is not None:
        if a.tag == W + "txbxContent" or a.tag == MC + "AlternateContent":
            return True
        if a.tag == W + "body":
            return False
        a = a.getparent()
    return False


def list_paragraphs(doc):
    """Body paragraphs in document order (table cells included, text-box
    content excluded)."""
    return [p for p in doc.element.body.iter(W + "p") if not _in_textbox(p)]


# ── Text views ───────────────────────────────────────────────────────

def _run_atoms(r):
    """[(child, visible_text)] for a run's content (rPr excluded)."""
    atoms = []
    for ch in r:
        if not _is_tag(ch) or ch.tag == W + "rPr":
            continue
        tag = ch.tag
        if tag in (W + "t", W + "delText"):
            atoms.append((ch, ch.text or ""))
        elif tag in (W + "tab", W + "ptab"):
            atoms.append((ch, "\t"))
        elif tag in (W + "br", W + "cr"):
            atoms.append((ch, "\n"))
        elif tag == W + "noBreakHyphen":
            atoms.append((ch, "‑"))
        else:
            atoms.append((ch, ""))
    return atoms


def _run_text(r):
    return "".join(t for _, t in _run_atoms(r))


def para_text(p, mode="accept"):
    """Visible text of one paragraph. mode: 'accept' (insertions shown,
    deletions hidden) or 'reject' (the reverse)."""
    parts = []

    def walk(el):
        for ch in el:
            if not _is_tag(ch):
                continue
            tag = ch.tag
            if tag == W + "r":
                parts.append(_run_text(ch))
            elif tag in _INS:
                if mode != "reject":
                    walk(ch)
            elif tag in _DEL:
                if mode == "reject":
                    walk(ch)
            elif tag in _CONTAINERS:
                walk(ch)
    walk(p)
    return "".join(parts)


def _mark(p):
    rpr = p.find(W + "pPr/" + W + "rPr")
    if rpr is None:
        return None
    if rpr.find(W + "ins") is not None or rpr.find(W + "moveTo") is not None:
        return "ins"
    if rpr.find(W + "del") is not None or rpr.find(W + "moveFrom") is not None:
        return "del"
    return None


def doc_view(paras, mode):
    """Paragraph texts as Word shows them after accept-all or reject-all,
    honoring paragraph-mark revisions (a deleted mark joins a paragraph to
    the next one)."""
    texts, carry = [], ""
    for p in paras:
        t = carry + para_text(p, mode)
        m = _mark(p)
        if (mode == "accept" and m == "del") or (mode == "reject" and m == "ins"):
            carry = t
            continue
        texts.append(t)
        carry = ""
    if carry:
        texts.append(carry)
    return texts


def count_revisions(docx_bytes):
    """(insertions, deletions) carried by the file's body."""
    doc = _load(docx_bytes)
    body = doc.element.body
    return (sum(1 for _ in body.iter(W + "ins", W + "moveTo")),
            sum(1 for _ in body.iter(W + "del", W + "moveFrom")))


# ── Accept all (baseline + clean deliverable) ───────────────────────

_BODY_MARKERS = {W + t for t in (
    "bookmarkStart", "bookmarkEnd", "permStart", "permEnd", "proofErr",
    "commentRangeStart", "commentRangeEnd", "moveFromRangeStart",
    "moveFromRangeEnd", "moveToRangeStart", "moveToRangeEnd",
    "customXmlInsRangeStart", "customXmlInsRangeEnd",
    "customXmlDelRangeStart", "customXmlDelRangeEnd")}


def _next_paragraph(p):
    """The paragraph that follows p, looking past body-level range markers
    (bookmarks, comment ranges, proofing marks) and into a block-level
    content control; None when a table or the section end comes first."""
    nxt, node = p.getnext(), p
    while True:
        if nxt is None:
            holder = node.getparent()
            if holder is not None and holder.tag == W + "sdtContent":
                node = holder.getparent()
                nxt = node.getnext()
                continue
            return None
        if not _is_tag(nxt) or nxt.tag in _BODY_MARKERS:
            nxt = nxt.getnext()
            continue
        if nxt.tag == W + "sdt":
            first = nxt.find(W + "sdtContent")
            if first is not None and len(first):
                node, nxt = first[0], first[0]
            else:
                node, nxt = nxt, nxt.getnext()
            continue
        return nxt if nxt.tag == W + "p" else None


def _merge_into_next(p):
    """Apply a deleted paragraph mark. A paragraph whose text is all gone
    simply disappears into the next one; a paragraph that still has text
    is joined with the next and keeps its own properties (numbering,
    style), as when two clauses are run together. A section break on the
    following paragraph's mark is carried over so layout survives."""
    nxt = _next_paragraph(p)
    if nxt is None:
        return False
    if not para_text(p).strip():
        content = [c for c in p if _is_tag(c) and c.tag != W + "pPr"]
        idx = 1 if (len(nxt) and nxt[0].tag == W + "pPr") else 0
        for c in content:
            nxt.insert(idx, c)
            idx += 1
        p.getparent().remove(p)
        return True
    for c in [c for c in nxt if _is_tag(c) and c.tag != W + "pPr"]:
        p.append(c)
    sect = nxt.find(W + "pPr/" + W + "sectPr")
    if sect is not None:
        ppr = p.find(W + "pPr")
        if ppr is None:
            ppr = _el("pPr")
            p.insert(0, ppr)
        for old_sect in ppr.findall(W + "sectPr"):
            ppr.remove(old_sect)
        ppr.append(sect)
    nxt.getparent().remove(nxt)
    return True


def _accept_tree(root):
    for tag in _RANGE_MARKS:
        for el in list(root.iter(W + tag)):
            el.getparent().remove(el)
    for tag in _PROPERTY_CHANGES:
        for el in list(root.iter(W + tag)):
            el.getparent().remove(el)
    for el in list(root.iter(W + "del", W + "moveFrom")):
        parent = el.getparent()
        if parent is not None and parent.tag not in _MARKER_PARENTS:
            parent.remove(el)
    for el in list(root.iter(W + "ins", W + "moveTo")):
        parent = el.getparent()
        if parent is not None and parent.tag not in _MARKER_PARENTS:
            _unwrap(el)
    for el in list(root.iter(W + "cellIns", W + "cellDel")):
        el.getparent().remove(el)
    # Revision markers. Paragraph marks are processed last-first so a run
    # of deleted marks collapses into the surviving paragraph.
    markers = [el for el in root.iter(W + "ins", W + "del", W + "moveFrom",
                                      W + "moveTo")
               if el.getparent() is not None
               and el.getparent().tag in _MARKER_PARENTS]
    for el in reversed(markers):
        holder = el.getparent()
        if holder is None:
            continue
        owner = holder.getparent()
        deleted = el.tag in (W + "del", W + "moveFrom")
        holder.remove(el)
        if not deleted or owner is None:
            continue
        if holder.tag == W + "trPr":
            row = owner
            if row.getparent() is not None:
                row.getparent().remove(row)
        elif holder.tag == W + "rPr" and owner.tag == W + "pPr":
            p = owner.getparent()
            if p is not None and p.tag == W + "p":
                _merge_into_next(p)
    # A table whose every row was deleted goes with its rows.
    for tbl in list(root.iter(W + "tbl")):
        if tbl.find(W + "tr") is None and tbl.getparent() is not None:
            tbl.getparent().remove(tbl)


def accept_all(docx_bytes):
    """Accept every tracked change (body, headers, footers, notes)."""
    from lxml import etree
    doc = _load(docx_bytes)
    _accept_tree(doc.element.body)
    for part in doc.part.package.iter_parts():
        if not _STORY_PART.match(str(part.partname)):
            continue
        if hasattr(part, "_element"):
            _accept_tree(part._element)
        else:
            # python-docx loads footnotes/endnotes as opaque parts.
            root = etree.fromstring(part.blob)
            _accept_tree(root)
            part._blob = etree.tostring(root, xml_declaration=True,
                                        encoding="UTF-8", standalone=True)
    return _save(doc)


_REVISION_TAG = re.compile(rb"<w:(ins|del|moveFrom|moveTo)[\s/>]")


def story_revisions(docx_bytes):
    """Live revision marks in every story part (body, headers, footers,
    notes), read straight from the package."""
    import zipfile
    total = 0
    with zipfile.ZipFile(io.BytesIO(docx_bytes)) as z:
        for name in z.namelist():
            if re.match(r"^word/(document|header\d*|footer\d*|footnotes|"
                        r"endnotes|comments)\.xml$", name):
                total += len(_REVISION_TAG.findall(z.read(name)))
    return total


# ── Numbering labels (so the reviewer can cite clause numbers) ───────

def _roman(n):
    vals = [(1000, "m"), (900, "cm"), (500, "d"), (400, "cd"), (100, "c"),
            (90, "xc"), (50, "l"), (40, "xl"), (10, "x"), (9, "ix"),
            (5, "v"), (4, "iv"), (1, "i")]
    out = ""
    for v, s in vals:
        while n >= v:
            out += s
            n -= v
    return out


def _fmt_num(n, fmt):
    if fmt in ("lowerLetter", "upperLetter"):
        s, k = "", n
        while k > 0:
            k, rem = divmod(k - 1, 26)
            s = chr(97 + rem) + s
        return s.upper() if fmt == "upperLetter" else s
    if fmt in ("lowerRoman", "upperRoman"):
        s = _roman(n)
        return s.upper() if fmt == "upperRoman" else s
    if fmt == "bullet":
        return "•"
    if fmt == "none":
        return ""
    return str(n)


def _val(el, path):
    if el is None:
        return None
    x = el.find(path)
    return None if x is None else x.get(W + "val")


def _lvl_def(lvl):
    return (_val(lvl, W + "numFmt") or "decimal",
            _val(lvl, W + "lvlText") or "",
            int(_val(lvl, W + "start") or "1"),
            lvl.find(W + "isLgl") is not None)


def numbering_info(doc, paras):
    """Best-effort rendering of Word auto-numbering: [(label, ilvl)] per
    paragraph, with ilvl None for paragraphs that are not numbered."""
    try:
        numbering = doc.part.numbering_part.element
    except Exception:
        numbering = None
    try:
        styles = {s.get(W + "styleId"): s
                  for s in doc.styles.element.iter(W + "style")}
    except Exception:
        styles = {}
    if numbering is None:
        return [("", None)] * len(paras)
    abstract, links, defines, nums = {}, {}, {}, {}
    for a in numbering.iter(W + "abstractNum"):
        aid = a.get(W + "abstractNumId")
        abstract[aid] = {int(l.get(W + "ilvl", "0")): _lvl_def(l)
                         for l in a.iter(W + "lvl")}
        link = _val(a, W + "numStyleLink")
        if link:
            links[aid] = link
        defined = _val(a, W + "styleLink")
        if defined:
            defines[defined] = aid
    for n in numbering.iter(W + "num"):
        nid = n.get(W + "numId")
        restarts, lvl_over = {}, {}
        for ov in n.iter(W + "lvlOverride"):
            k = int(ov.get(W + "ilvl", "0"))
            so = _val(ov, W + "startOverride")
            if so is not None:
                restarts[k] = int(so)
            lv = ov.find(W + "lvl")
            if lv is not None:
                lvl_over[k] = _lvl_def(lv)
                restarts.setdefault(k, lvl_over[k][2])
        nums[nid] = (_val(n, W + "abstractNumId"), restarts, lvl_over)

    def style_num_id(style_id):
        st = styles.get(style_id)
        return _val(st, W + "pPr/" + W + "numPr/" + W + "numId") \
            if st is not None else None

    def resolve_levels(abs_id, hops=0):
        # A numStyleLink abstract borrows its levels from the list style.
        if abs_id in links and hops < 4:
            target = defines.get(links[abs_id])
            if target is None:
                nid = style_num_id(links[abs_id])
                target = nums[nid][0] if nid in nums else None
            if target is not None and target != abs_id:
                return resolve_levels(target, hops + 1)
        return abs_id, abstract.get(abs_id, {})

    def para_num(p):
        # numId and ilvl resolve independently through the style chain.
        ppr = p.find(W + "pPr")
        nid = _val(ppr, W + "numPr/" + W + "numId")
        ilvl = _val(ppr, W + "numPr/" + W + "ilvl")
        style_id, hops = _val(ppr, W + "pStyle"), 0
        while (nid is None or ilvl is None) and style_id and hops < 8:
            st = styles.get(style_id)
            if st is None:
                break
            if nid is None:
                nid = _val(st, W + "pPr/" + W + "numPr/" + W + "numId")
            if ilvl is None:
                ilvl = _val(st, W + "pPr/" + W + "numPr/" + W + "ilvl")
            style_id = _val(st, W + "basedOn")
            hops += 1
        return nid, int(ilvl or "0")

    # Word keeps one counter per abstract list; a num with a startOverride
    # restarts that list the first time the num is used.
    counters, restarted, out = {}, set(), []
    for p in paras:
        nid, ilvl = para_num(p)
        if not nid or nid == "0" or nid not in nums:
            out.append(("", None))
            continue
        abs_id, restarts, lvl_over = nums[nid]
        abs_id, levels = resolve_levels(abs_id)
        levels = {**levels, **lvl_over}
        cnt = counters.setdefault(abs_id, [None] * 9)
        ilvl = min(max(ilvl, 0), 8)
        start = levels.get(ilvl, ("decimal", "", 1, False))[2]
        if ilvl in restarts and (nid, ilvl) not in restarted:
            restarted.add((nid, ilvl))
            cnt[ilvl] = restarts[ilvl]
        else:
            cnt[ilvl] = start if cnt[ilvl] is None else cnt[ilvl] + 1
        for j in range(ilvl):
            if cnt[j] is None:
                cnt[j] = levels.get(j, ("decimal", "", 1, False))[2]
        for j in range(ilvl + 1, 9):
            cnt[j] = None
        fmt, text, _, legal = levels.get(ilvl, ("decimal", "%1.", 1, False))
        if fmt == "bullet":
            out.append(("\u2022", ilvl))
            continue

        def sub(m):
            k = int(m.group(1)) - 1
            lf = levels.get(k, ("decimal", "", 1, False))[0]
            return _fmt_num(cnt[k] if cnt[k] is not None else 1,
                            "decimal" if legal else lf)
        out.append((re.sub(r"%(\d)", sub, text).strip(), ilvl))
    return out


def numbering_labels(doc, paras):
    return [label for label, _ in numbering_info(doc, paras)]


# ── Baseline view for the reviewer ──────────────────────────────────

def _curly_share(texts):
    joined = "".join(texts)
    curly = sum(joined.count(c) for c in "“”‘’")
    straight = joined.count('"') + joined.count("'")
    return curly >= straight and curly > 0


def signature_start(texts):
    for i, t in enumerate(texts):
        if SIGNATURE_CUE.match(t or ""):
            return i
    return len(texts)


_CLOSING_CUE = re.compile(
    r"^\s*(please\b|kindly\b|if (this|the foregoing)\b|very truly|sincerely|"
    r"in witness|(schedule|exhibit|annex|appendix|attachment)\b)", re.I)


def default_insert_anchor(texts, labels, section_ends=None):
    """Where a new clause goes when no better spot is given: at the end of
    the last operative clause (its heading and any body paragraphs), before
    closing lines, schedules, a section break and the signature block."""
    sig = signature_start(texts)
    ends = section_ends or [False] * len(texts)
    numbered = [i for i in range(sig) if labels[i] and labels[i] != "\u2022"]
    anchor = numbered[-1] if numbered else None
    start = (anchor + 1) if anchor is not None else 0
    for k in range(start, sig):
        if anchor is not None and ends[anchor]:
            break
        t = (texts[k] or "").strip()
        if not t:
            if ends[k]:
                break
            continue
        if _CLOSING_CUE.match(t) or DELIVERY_CUE.match(t):
            break
        anchor = k
    if anchor is None:
        body = [i for i in range(sig) if (texts[i] or "").strip()]
        anchor = body[-1] if body else max(0, len(texts) - 1)
    return anchor


def baseline_view(source_bytes):
    """Everything the reviewer and the planner need about the baseline."""
    base = accept_all(source_bytes)
    doc = _load(base)
    paras = list_paragraphs(doc)
    texts = [para_text(p) for p in paras]
    info = numbering_info(doc, paras)
    labels = [lab for lab, _ in info]
    section_ends = [p.find(W + "pPr/" + W + "sectPr") is not None
                    for p in paras]
    return {
        "baseline": base,
        "texts": texts,
        "labels": labels,
        "levels": [lvl for _, lvl in info],
        "signature_start": signature_start(texts),
        "default_anchor": default_insert_anchor(texts, labels, section_ends),
        "curly_quotes": _curly_share(texts),
    }


def format_for_model(view):
    """The document as the reviewer sees it: one line per non-empty
    paragraph, tagged with its paragraph number and any auto-number."""
    lines = []
    sig = view["signature_start"]
    for i, (t, lab) in enumerate(zip(view["texts"], view["labels"])):
        if not (t or "").strip():
            continue
        tag = f"[¶{i}"
        if lab:
            tag += f" | auto-numbered {lab}"
        if i == sig:
            tag += " | signature block starts"
        lines.append(f"{tag}] {t.replace(chr(10), LINE_BREAK_MARK)}")
    return "\n".join(lines)


# ── Planning ─────────────────────────────────────────────────────────

def smarten_quotes(text):
    out = []
    for i, ch in enumerate(text):
        prev = text[i - 1] if i else " "
        opening = prev.isspace() or prev in "([{—–-/"
        if ch == '"':
            out.append("“" if opening else "”")
        elif ch == "'":
            out.append("‘" if opening and (i + 1 < len(text)
                                                 and text[i + 1].isalpha())
                       and not (i + 1 < len(text) and text[i + 1].isdigit())
                       else "’")
        else:
            out.append(ch)
    return "".join(out)


def _loose_pattern(s):
    out = []
    for ch in s:
        if ch.isspace():
            if not out or out[-1] != r"\s+":
                out.append(r"\s+")
        elif ch in "\"“”„":
            out.append("[\"“”„]")
        elif ch in "'‘’":
            out.append("['‘’]")
        else:
            out.append(re.escape(ch))
    return "".join(out)


_CONTROL = re.compile("[\x00-\x08\x0e-\x1f\x7f\ud800-\udfff\ufffe\uffff]")


def clean_text(text):
    """Make reviewer text safe for Word XML: Windows and old-Mac line
    endings, vertical tabs, form feeds and Unicode line separators become
    line breaks; other control characters are dropped."""
    t = (text if isinstance(text, str) else str(text or "")).replace(
        "\r\n", "\n")
    t = re.sub(r"[\r\x0b\x0c\u2028\u2029]", "\n", t)
    return _CONTROL.sub("", t)


def _keep_edges(new, old):
    """Carry the original paragraph's leading and trailing whitespace (a
    tab indent, a trailing space) onto the revised text, so trimming the
    reviewer's text never shows up as a tracked deletion."""
    lead = re.match(r"^\s*", old).group()
    trail = re.search(r"\s*$", old).group() if old.strip() else ""
    return lead + new.strip() + trail


def _strip_label(text, label):
    t = (text or "").strip()
    if label:
        for form in (f"({label})", label):
            if t.startswith(form + " "):
                return t[len(form):].lstrip()
    return t


DELIVERY_CUE = re.compile(
    r"^\s*(please\s+(complete|sign|execute|return|e-?mail|send|countersign)|"
    r"kindly\s+(sign|return)|return\s+(a|one)\s+(signed|executed))", re.I)
# A "new paragraph" that opens like this is really a sentence belonging to
# the clause before it; standing alone, its "this paragraph" would point
# at itself.
_CONTINUATION = re.compile(
    r"^\s*(this\s+paragraph|this\s+section|such\b|the\s+foregoing|"
    r"notwithstanding\s+the\s+foregoing|in\s+addition|for\s+the\s+avoidance"
    r"\s+of\s+doubt|provided(,)?\s+(however|that)|the\s+recipient['’]s"
    r"\s+counsel)", re.I)


def new_plan():
    return {"modify": {}, "delete": [], "insert": []}


def final_view(view, plan):
    """NCP's position as it will read once every change is accepted,
    tagged with the baseline paragraph numbers it came from."""
    texts = view["texts"]
    inserted = {}
    for ins in plan.get("insert", []):
        inserted.setdefault(ins["after"], []).append(ins["text"])
    deleted = set(plan.get("delete", []))
    lines = [f"[new clause] {t}" for t in inserted.get(-1, [])]
    for i, t in enumerate(texts):
        if i in deleted:
            lines.append(f"[¶{i} deleted]")
        else:
            body = plan.get("modify", {}).get(i, t)
            if body.strip():
                tag = f"¶{i} revised" if i in plan.get("modify", {}) else f"¶{i}"
                lines.append(f"[{tag}] {body.replace(chr(10), LINE_BREAK_MARK)}")
        lines.extend(f"[new after ¶{i}] {x.replace(chr(10), LINE_BREAK_MARK)}"
                     for x in inserted.get(i, []))
    return "\n".join(lines)


def plan_from_edits(edits, view):
    """Paragraph-keyed reviewer edits -> plan. Returns (plan, notes)."""
    texts, labels = view["texts"], view["labels"]
    n = len(texts)
    sig = view["signature_start"]
    plan, notes, pending_append = new_plan(), [], []
    for e in edits if isinstance(edits, list) else []:
        if not isinstance(e, dict):
            notes.append("An edit that was not a JSON object was skipped.")
            continue
        action = str(e.get("action") or "").strip().lower()
        try:
            i = int(e.get("para"))
        except (TypeError, ValueError):
            notes.append(f"Edit without a valid paragraph number skipped: "
                         f"{(e.get('text') or '')[:80]}")
            continue
        text = re.sub(r" ?⏎ ?", "\n", clean_text(e.get("text")))
        if view.get("curly_quotes"):
            text = smarten_quotes(text)
        if action == "modify":
            if not 0 <= i < n:
                notes.append(f"¶{i} does not exist; edit skipped.")
                continue
            if i in plan["modify"]:
                notes.append(f"Two edits targeted ¶{i}; the later one was used.")
            plan["modify"][i] = _keep_edges(_strip_label(text, labels[i]),
                                            texts[i])
        elif action == "delete":
            if 0 <= i < n:
                plan["delete"].append(i)
        elif action in ("insert_after", "insert"):
            body = text.strip()
            if not body:
                continue
            numbered = e.get("numbered")
            if isinstance(numbered, str) and numbered != "same_level":
                numbered = numbered.strip().lower() in ("true", "yes", "1")
            if not -1 <= i < n:
                i, numbered = view["default_anchor"], True
            if SIGNATURE_CUE.match(body):
                delivery = next((k for k in range(max(i, 0), n)
                                 if DELIVERY_CUE.match(texts[k] or "")), None)
                if delivery is not None and i >= delivery:
                    before = max([k for k in range(delivery)
                                  if (texts[k] or "").strip()] or [i])
                    notes.append(f"The acceptance block was moved above the "
                                 f"delivery instructions at ¶{delivery}.")
                    i = before
            elif i >= sig:
                notes.append(f"A new clause anchored inside the signature "
                             f"block (after ¶{i}) was moved to follow "
                             f"¶{view['default_anchor']}, the last operative "
                             f"clause.")
                i, numbered = view["default_anchor"], True
            elif i >= 0 and _CONTINUATION.match(body) and \
                    (texts[i] or "").strip():
                pending_append.append((i, body))
                notes.append(f"A sentence emitted as its own paragraph after "
                             f"¶{i} reads as part of that clause, so it was "
                             f"added to the end of ¶{i} instead.")
                continue
            plan["insert"].append({"after": i, "text": body,
                                   "numbered": numbered})
        else:
            notes.append(f"Unknown edit action '{action}' skipped.")
    for i, body in pending_append:
        base = plan["modify"].get(i, texts[i]).rstrip()
        plan["modify"][i] = f"{base} {body}"
    plan["delete"] = [i for i in sorted(set(plan["delete"]))
                      if i not in plan["modify"]]
    return plan, notes


def plan_from_legacy(edits, view):
    """Substring edits from rounds reviewed before the paragraph-keyed
    format -> plan. Returns (plan, skipped)."""
    texts = view["texts"]
    work = {}
    plan, skipped = new_plan(), []

    def cur(i):
        return work.get(i, texts[i])

    edits = [e for e in (edits if isinstance(edits, list) else [])
             if isinstance(e, dict)]
    for e in edits:
        orig = str(e.get("original") or "").strip()
        revised = clean_text(e.get("revised") or "")
        if view.get("curly_quotes"):
            revised = smarten_quotes(revised)
        if not orig:
            continue
        lines = [l.strip() for l in orig.split("\n") if l.strip()]
        if len(lines) > 1:
            norm = [re.sub(r"\s+", " ", l) for l in lines]
            hit = None
            for i in range(len(texts) - len(lines) + 1):
                if [re.sub(r"\s+", " ", cur(i + k).strip())
                        for k in range(len(lines))] == norm:
                    hit = i
                    break
            if hit is None:
                skipped.append({**e, "_skip_reason":
                                "multi-paragraph text not found"})
                continue
            work[hit] = revised
            for k in range(1, len(lines)):
                plan["delete"].append(hit + k)
            continue
        pat = re.compile(_loose_pattern(orig))
        hits = [(i, m) for i in range(len(texts))
                for m in [pat.search(cur(i))] if m]
        total = sum(len(pat.findall(cur(i))) for i, _ in hits)
        if total != 1:
            skipped.append({**e, "_skip_reason":
                            f"original text found {total} times "
                            f"(needs exactly 1)"})
            continue
        i, m = hits[0]
        work[i] = cur(i)[:m.start()] + revised + cur(i)[m.end():]
    for i, t in work.items():
        plan["modify"][i] = t
    for e in edits:
        if str(e.get("original") or "").strip():
            continue
        body = clean_text(e.get("revised") or "").strip()
        if not body:
            continue
        if view.get("curly_quotes"):
            body = smarten_quotes(body)
        anchor = str(e.get("insert_after") or "").strip()
        i, numbered = view["default_anchor"], True
        if anchor:
            pat = re.compile(_loose_pattern(anchor))
            found = [k for k in range(len(texts)) if pat.search(cur(k))]
            if found and found[0] < view["signature_start"]:
                i = found[0]
        plan["insert"].append({"after": i, "text": body,
                               "numbered": numbered})
    plan["delete"] = [i for i in sorted(set(plan["delete"]))
                      if i not in plan["modify"]]
    return plan, skipped


# ── Word-level comparison ────────────────────────────────────────────

_TOKEN = re.compile(r"\s+|\w+|[^\w\s]", re.UNICODE)
_QMAP = str.maketrans({"“": '"', "”": '"', "„": '"',
                       "‘": "'", "’": "'", "‑": "-"})


def _norm_tok(t):
    return " " if t.isspace() else t.translate(_QMAP)


def norm_text(s):
    return "".join(_norm_tok(t) for t in _TOKEN.findall(s or "")).strip()


_GLUE_WORDS = {"the", "a", "an", "of", "and", "or", "to", "in", "for", "by",
               "with", "on", "at", "its", "their", "any", "such"}


def _is_glue(tokens):
    """Unchanged text short and plain enough to fold into the surrounding
    change so the banker reads whole phrases: plain spaces, light
    punctuation and up to two function words. Defined terms, tabs and line
    breaks are never folded, so their formatting and layout survive."""
    words = 0
    for t in tokens:
        if t.isspace():
            if set(t) != {" "}:
                return False
        elif re.match(r"\w", t):
            if t.lower() not in _GLUE_WORDS:
                return False
            words += 1
        elif t not in ",;:.()":
            return False
    return words <= 2 and sum(len(t) for t in tokens) <= 14


def diff_ops(old, new):
    """Word-level edit script in character coordinates.

    Returns (ops, merged) where ops are (tag, a0, a1, b0, b1) with tag in
    equal/delete/insert/replace, and merged is the paragraph text after
    accepting (equal spans keep the counterparty's exact characters)."""
    ot, nt = _TOKEN.findall(old), _TOKEN.findall(new)
    on, nn = [_norm_tok(t) for t in ot], [_norm_tok(t) for t in nt]
    sm = difflib.SequenceMatcher(None, on, nn, autojunk=False)
    ops = sm.get_opcodes()
    # Absorb short unchanged glue ("the", "of the") sitting between two
    # changes so the banker reads whole phrases, not confetti.
    changed = True
    while changed:
        changed, out, k = False, [], 0
        while k < len(ops):
            if (out and k + 1 < len(ops) and ops[k][0] == "equal"
                    and out[-1][0] != "equal" and ops[k + 1][0] != "equal"
                    and _is_glue(ot[ops[k][1]:ops[k][2]])):
                prev, nxt = out.pop(), ops[k + 1]
                out.append(("replace", prev[1], nxt[2], prev[3], nxt[4]))
                k += 2
                changed = True
                continue
            out.append(ops[k])
            k += 1
        ops = out
    # Heavily rewritten paragraph: one deletion and one insertion for the
    # changed region (the rulebook's fix for fragmented compare output).
    non_equal = [o for o in ops if o[0] != "equal"]
    if len(non_equal) > 1 and sm.ratio() < 0.45:
        head = ops[0] if ops[0][0] == "equal" else None
        tail = ops[-1] if ops[-1][0] == "equal" and len(ops) > 1 else None
        a0 = head[2] if head else 0
        b0 = head[4] if head else 0
        a1 = tail[1] if tail else len(ot)
        b1 = tail[3] if tail else len(nt)
        ops = ([head] if head else []) + [("replace", a0, a1, b0, b1)] + \
              ([tail] if tail else [])

    def offsets(toks):
        acc, pos = [], 0
        for t in toks:
            acc.append(pos)
            pos += len(t)
        acc.append(pos)
        return acc
    oo, no = offsets(ot), offsets(nt)
    char_ops, merged = [], []
    for tag, i1, i2, j1, j2 in ops:
        a0, a1, b0, b1 = oo[i1], oo[i2], no[j1], no[j2]
        if tag == "replace" and a0 == a1:
            tag = "insert"
        elif tag == "replace" and b0 == b1:
            tag = "delete"
        char_ops.append((tag, a0, a1, b0, b1))
        merged.append(old[a0:a1] if tag == "equal" else new[b0:b1])
    return char_ops, "".join(merged)


# ── Writing tracked changes into the counterparty's runs ────────────

class _Ctx:
    def __init__(self, author, date):
        self.author, self.date, self.next_id = author, date, ID_START

    def rev(self, kind):
        el = _el(kind)
        el.set(W + "id", str(self.next_id))
        el.set(W + "author", self.author)
        el.set(W + "date", self.date)
        self.next_id += 1
        return el


def _collect_runs(p):
    """[(run, start, end, top)] in reading order; top is the paragraph's
    direct child that holds the run (the run itself, or e.g. a hyperlink)."""
    out, pos = [], [0]

    def walk(el, top):
        for ch in el:
            if not _is_tag(ch):
                continue
            if ch.tag == W + "r":
                n = len(_run_text(ch))
                out.append((ch, pos[0], pos[0] + n,
                            ch if top is None else top))
                pos[0] += n
            elif ch.tag in _CONTAINERS or ch.tag in _INS:
                walk(ch, ch if top is None else top)
    walk(p, None)
    return out


_VOLATILE_FIELDS = {"DATE", "TIME", "PRINTDATE", "SAVEDATE", "CREATEDATE",
                    "EDITTIME", "REVNUM", "SEQ", "AUTONUM", "AUTONUMLGL",
                    "AUTONUMOUT", "LISTNUM"}
_OBJECT_TAGS = {W + "drawing", W + "pict", W + "object", MC + "AlternateContent",
                W + "txbxContent"}
# Zero-width run content that still prints (and so must go when its
# clause is deleted).
_PRINTING_ATOMS = {W + "footnoteReference", W + "endnoteReference", W + "sym",
                   W + "drawing", W + "pict", W + "object",
                   MC + "AlternateContent", W + "commentReference"}
# Elements whose content belongs to the counterparty's object (a link, a
# content control, a field): new text at their edge goes outside them.
_WRAPPER_TAGS = {W + "hyperlink", W + "sdt", W + "fldSimple"}


def _outside_objects(el, p):
    a = el.getparent()
    while a is not None and a is not p:
        if a.tag in _OBJECT_TAGS:
            return False
        a = a.getparent()
    return True


def _complex_fields(runs):
    """Complex fields among a paragraph's runs: dicts with the field code,
    the runs that make it up, its displayed-result character span and its
    begin/end runs. Only fields wholly inside the paragraph are returned."""
    stack, done = [], []
    for idx, (r, rs, re_, _) in enumerate(runs):
        for ch in r:
            if not _is_tag(ch):
                continue
            if ch.tag == W + "fldChar":
                kind = ch.get(W + "fldCharType")
                if kind == "begin":
                    stack.append({"runs": [r], "code": "", "begin": r,
                                  "start": rs, "first": idx, "sep": None})
                elif kind == "separate" and stack:
                    stack[-1]["runs"].append(r)
                    stack[-1]["start"] = re_
                    stack[-1]["sep"] = idx
                elif kind == "end" and stack:
                    f = stack.pop()
                    f.update(end=r, stop=rs, last=idx)
                    f["runs"].append(r)
                    done.append(f)
            elif ch.tag == W + "instrText" and stack:
                stack[-1]["code"] += ch.text or ""
                stack[-1]["runs"].append(r)
    for f in done:
        f["members"] = set(range((f["sep"] if f["sep"] is not None
                                  else f["first"]) + 1, f["last"]))
    return done


def _touches(start, end, ops):
    """Does the edit script delete inside, or insert strictly inside, the
    character span [start, end)?"""
    for tag, a0, a1, b0, b1 in ops or []:
        if tag in ("delete", "replace") and a1 > a0 and a0 < end and a1 > start:
            return True
        x = a1 if tag == "replace" else a0
        if tag in ("insert", "replace") and b1 > b0 and start < x < end:
            return True
    return False


def _collapse_fields(p, ops=None):
    """Turn fields into their displayed text where they would otherwise
    misbehave: volatile fields (DATE, SEQ and the like, which the rulebook
    says surface as spurious changes) and any field whose displayed result
    this edit changes (or Word would regenerate it and undo NCP's words).
    Other fields, and anything inside text boxes or drawings, keep working."""
    runs = _collect_runs(p)
    for fs in [x for x in p.iter(W + "fldSimple") if _outside_objects(x, p)]:
        span = [(rs, re_) for r, rs, re_, _ in runs if fs in r.iterancestors()]
        start = min((s for s, _ in span), default=0)
        end = max((e for _, e in span), default=0)
        code = (fs.get(W + "instr") or "").split()
        if (code and code[0].upper() in _VOLATILE_FIELDS) or \
                _touches(start, end, ops):
            _unwrap(fs)
    for f in _complex_fields(_collect_runs(p)):
        code = f["code"].split()
        if not ((code and code[0].upper() in _VOLATILE_FIELDS)
                or _touches(f["start"], f["stop"], ops)):
            continue
        for r in f["runs"]:
            for ch in list(r):
                if _is_tag(ch) and ch.tag in (W + "fldChar", W + "instrText"):
                    r.remove(ch)
            if r.getparent() is not None and \
                    not [c for c in r if _is_tag(c) and c.tag != W + "rPr"]:
                r.getparent().remove(r)


def _clean_rpr(rpr):
    if rpr is None:
        return None
    rpr = copy.deepcopy(rpr)
    for ch in list(rpr):
        if not _is_tag(ch):
            continue
        if ch.tag in (W + "ins", W + "del", W + "moveFrom", W + "moveTo",
                      W + "rPrChange"):
            rpr.remove(ch)
        elif ch.tag == W + "rStyle" and (ch.get(W + "val") or "").lower() in (
                "hyperlink", "followedhyperlink"):
            rpr.remove(ch)
    return rpr


def _emphasized(r):
    rpr = r.find(W + "rPr") if r is not None else None
    if rpr is None:
        return False
    for tag in ("b", "i", "u", "caps", "smallCaps", "strike", "highlight"):
        el = rpr.find(W + tag)
        if el is not None and (el.get(W + "val") or "true").lower() not in (
                "0", "false", "none", "off"):
            return True
    style = rpr.find(W + "rStyle")
    return style is not None and "hyperlink" in (style.get(W + "val") or
                                                 "").lower()


def _text_run(text, rpr):
    r = _el("r")
    if rpr is not None:
        r.append(copy.deepcopy(rpr))
    for piece in re.split(r"(\t|\n)", text):
        if not piece:
            continue
        if piece == "\t":
            r.append(_el("tab"))
        elif piece == "\n":
            r.append(_el("br"))
        else:
            t = _el("t")
            t.text = piece
            t.set(XML_SPACE, "preserve")
            r.append(t)
    return r


def _segment_run(r, rs, bounds):
    """Rebuild run r as ordered segments [(start, end, new_run, is_atom)]:
    text cut at `bounds`, and every zero-width atom (footnote mark, symbol,
    image, field character) in a segment of its own so it is deleted only
    when it sits strictly inside a deletion."""
    rpr = r.find(W + "rPr")

    def fresh():
        nr = _el("r")
        for k, v in r.attrib.items():
            nr.set(k, v)
        if rpr is not None:
            nr.append(copy.deepcopy(rpr))
        return nr

    segs, cur, pos = [], None, rs
    for child, txt in _run_atoms(r):
        n = len(txt)
        if n == 0:
            if child.tag == W + "lastRenderedPageBreak":
                continue
            nr = fresh()
            nr.append(copy.deepcopy(child))
            segs.append([pos, pos, nr, True])
            cur = None
            continue
        cuts = [pos] + [b for b in bounds if pos < b < pos + n] + [pos + n]
        for lo, hi in zip(cuts, cuts[1:]):
            if cur is None or cur[1] != lo or any(b == lo for b in bounds):
                cur = [lo, lo, fresh(), False]
                segs.append(cur)
            if child.tag in (W + "t", W + "delText"):
                t = _el("t")
                t.text = txt[lo - pos:hi - pos]
                t.set(XML_SPACE, "preserve")
                cur[2].append(t)
            else:
                cur[2].append(copy.deepcopy(child))
            cur[1] = hi
        pos += n
    return [tuple(s) for s in segs]


def _as_deleted(r):
    for ch in list(r):
        if not _is_tag(ch):
            continue
        if ch.tag == W + "t":
            d = _el("delText")
            d.text = ch.text
            d.set(XML_SPACE, "preserve")
            r.replace(ch, d)
        elif ch.tag == W + "instrText":
            d = _el("delInstrText")
            d.text = ch.text
            r.replace(ch, d)
    return r


def _rpr_key(r):
    rpr = r.find(W + "rPr")
    if rpr is None:
        return b""
    from lxml import etree
    return etree.tostring(_clean_rpr(rpr))


def _apply_ops(p, old, ops, ctx, delete_all=False):
    """Rewrite paragraph p in place so it carries the edit script.

    Returns (runs, segments_by_run, inserts, wrappers) for
    _place_insertions; wrappers are measured before any run is replaced.
    With delete_all every run goes, including zero-width ones."""
    runs = _collect_runs(p)
    wraps = _wrapper_spans(p, runs)
    cuts = set()
    deleted, inserts = [], []
    for tag, a0, a1, b0, b1 in ops:
        cuts.update((a0, a1))
        if tag in ("delete", "replace") and a1 > a0:
            deleted.append((a0, a1))
        if tag in ("insert", "replace") and b1 > b0:
            inserts.append((a1 if tag == "replace" else a0, (b0, b1),
                            (a0, a1) if tag == "replace" else None))

    def is_del(s, e, atom):
        if delete_all:
            return True
        if atom:
            return any(a0 < s < a1 for a0, a1 in deleted)
        return any(a0 <= s and e <= a1 for a0, a1 in deleted)

    segs_of = {}
    for r, rs, re_, top in runs:
        if re_ == rs:
            if delete_all or (any(_is_tag(c) and c.tag in _PRINTING_ATOMS
                                  for c in r)
                              and any(a0 < rs < a1 for a0, a1 in deleted)):
                d = ctx.rev("del")
                r.getparent().replace(r, d)
                d.append(_as_deleted(r))
            continue
        bounds = sorted(c for c in cuts if rs < c < re_)
        has_atoms = any(t == "" and c.tag != W + "lastRenderedPageBreak"
                        for c, t in _run_atoms(r))
        if not bounds and not has_atoms and not is_del(rs, re_, False):
            segs_of[id(r)] = [(rs, re_, r, False)]
            continue
        elems = []
        for s, e, nr, atom in _segment_run(r, rs, bounds):
            if is_del(s, e, atom):
                d = ctx.rev("del")
                d.append(_as_deleted(nr))
                elems.append((s, e, d, atom))
            else:
                elems.append((s, e, nr, atom))
        parent = r.getparent()
        at = parent.index(r)
        parent.remove(r)
        for k, (_, _, el, _) in enumerate(elems):
            parent.insert(at + k, el)
        segs_of[id(r)] = elems
    return runs, segs_of, inserts, wraps


def _wrapper_spans(p, runs):
    """Per run: the wrappers around it as (start, end, before_el, after_el),
    outermost last. Wrappers are hyperlinks, content controls and simple
    fields (elements) and complex-field results (begin/end runs)."""
    spans = {}
    for r, rs, re_, _ in runs:
        a = r.getparent()
        while a is not None and a is not p:
            if a.tag in _WRAPPER_TAGS:
                sp = spans.setdefault(id(a), [rs, re_, a])
                sp[0], sp[1] = min(sp[0], rs), max(sp[1], re_)
            a = a.getparent()
    per_run = {}
    for idx, (r, rs, re_, _) in enumerate(runs):
        ws = []
        a = r.getparent()
        while a is not None and a is not p:
            if a.tag in _WRAPPER_TAGS:
                s, e, el = spans[id(a)]
                ws.append((s, e, el, el))
            a = a.getparent()
        per_run[id(r)] = ws
    for f in _complex_fields(runs):
        for idx in f["members"]:
            r = runs[idx][0]
            per_run.setdefault(id(r), []).append(
                (f["start"], f["stop"], f["begin"], f["end"]))
    for ws in per_run.values():
        ws.sort(key=lambda w: w[1] - w[0])
    return per_run


def _place_insertions(p, old, new, runs, segs_of, inserts, ctx, wraps):
    text_runs = [x for x in runs if x[2] > x[1]]
    weights = {}
    for r, rs, re_, _ in text_runs:
        k = _rpr_key(r)
        weights[k] = weights.get(k, 0) + (re_ - rs)
    dominant = max(weights, key=weights.get) if weights else b""
    last_after = {}

    def run_at(x, after):
        for r, rs, re_, t in text_runs:
            if (rs < x <= re_) if after else (rs <= x < re_):
                return r
        return None

    def put_after(el, ins):
        # Keyed by id() but holding the element, so the key cannot be
        # reused by a different element mid-loop.
        held = last_after.get(id(el))
        (held[1] if held and held[0] is el else el).addnext(ins)
        last_after[id(el)] = (el, ins)

    for x, (b0, b1), replaced in inserts:
        text = new[b0:b1]
        ins = ctx.rev("ins")
        if replaced is not None:
            # A replacement keeps the look of what it replaces and goes
            # right after the deleted text.
            a0, a1 = replaced
            covering = [(min(re_, a1) - max(rs, a0), r)
                        for r, rs, re_, _ in text_runs if rs < a1 and re_ > a0]
            src = max(covering, key=lambda c: c[0])[1] if covering else None
            ins.append(_text_run(text, _clean_rpr(
                src.find(W + "rPr") if src is not None else None)))
            prev = run_at(x, True)
            if prev is not None:
                target = [el for s, e, el, atom in segs_of[id(prev)]
                          if e == x and not atom][-1]
                put_after(target, ins)
                continue
        prev, nxt = run_at(x, True), run_at(x, False)
        if prev is None and nxt is None:
            ins.append(_text_run(text, _clean_rpr(
                p.find(W + "pPr/" + W + "rPr"))))
            if len(p):
                put_after(p[len(p) - 1], ins)
            else:
                p.append(ins)
            continue
        if prev is not None and nxt is not None and prev is not nxt:
            pe, ne = _emphasized(prev), _emphasized(nxt)
            pw = any(w[1] == x for w in wraps.get(id(prev), []))
            nw = any(w[0] == x for w in wraps.get(id(nxt), []))
            if pe != ne:
                use_prev = not pe
            elif pw != nw:
                use_prev = not pw
            else:
                use_prev = (_rpr_key(prev) == dominant
                            or _rpr_key(nxt) != dominant)
        else:
            use_prev = prev is not None
        anchor = prev if use_prev else nxt
        ins.append(_text_run(text, _clean_rpr(anchor.find(W + "rPr"))))
        if use_prev:
            target = [el for s, e, el, _ in segs_of[id(prev)] if e == x][-1]
            exits = [w for w in wraps.get(id(prev), []) if w[1] == x]
            if exits:
                target = exits[-1][3]
            put_after(target, ins)
        else:
            target = [el for s, e, el, _ in segs_of[id(nxt)] if s == x][0]
            exits = [w for w in wraps.get(id(nxt), []) if w[0] == x]
            if exits:
                target = exits[-1][2]
            target.addprevious(ins)


def _mark_paragraph(p, ctx, kind):
    ppr = p.find(W + "pPr")
    if ppr is None:
        ppr = _el("pPr")
        p.insert(0, ppr)
    rpr = ppr.find(W + "rPr")
    if rpr is None:
        rpr = _el("rPr")
        before = None
        for tag in ("sectPr", "pPrChange"):
            before = ppr.find(W + tag)
            if before is not None:
                break
        if before is not None:
            before.addprevious(rpr)
        else:
            ppr.append(rpr)
    rpr.insert(0, ctx.rev(kind))


def _caption_rpr(p):
    """(caption_rPr, body_rPr) when the clause opens with a bold/underlined
    caption like 'Confidentiality.' and continues in body text."""
    runs = [x for x in _collect_runs(p) if x[2] > x[1]]
    if len(runs) < 2:
        return None
    first = runs[0][0]
    rpr = first.find(W + "rPr")
    if rpr is None:
        return None
    b = rpr.find(W + "b")
    u = rpr.find(W + "u")
    emphasized = (b is not None and b.get(W + "val") not in ("0", "false")) or \
                 (u is not None and u.get(W + "val") not in (None, "none"))
    text = _run_text(first).strip()
    if not emphasized or not text or len(text) > 80 or text[-1] not in ".:":
        return None
    body = next((x[0] for x in runs[1:] if _rpr_key(x[0]) != _rpr_key(first)),
                None)
    if body is None:
        return None
    return _clean_rpr(rpr), _clean_rpr(body.find(W + "rPr"))


def _new_paragraph(template, text, ctx):
    p = _el("p")
    tppr = template.find(W + "pPr") if template is not None else None
    if tppr is not None:
        ppr = copy.deepcopy(tppr)
        # Never inherit a neighbor's section break, fill-in underline
        # border, forced page break or text frame.
        for tag in ("sectPr", "pPrChange", "pBdr", "pageBreakBefore",
                    "framePr"):
            for el in ppr.findall(W + tag):
                ppr.remove(el)
        prpr = ppr.find(W + "rPr")
        if prpr is not None:
            for ch in list(prpr):
                if _is_tag(ch) and ch.tag in (W + "ins", W + "del",
                                              W + "moveFrom", W + "moveTo",
                                              W + "rPrChange"):
                    prpr.remove(ch)
        p.append(ppr)
    _mark_paragraph(p, ctx, "ins")
    runs = [x for x in _collect_runs(template) if x[2] > x[1]] \
        if template is not None else []
    weights = {}
    for r, rs, re_, _ in runs:
        k = _rpr_key(r)
        weights.setdefault(k, [0, r])[0] += re_ - rs
    body_rpr = None
    if weights:
        body_rpr = _clean_rpr(max(weights.values(), key=lambda v: v[0])[1]
                              .find(W + "rPr"))
    elif tppr is not None:
        body_rpr = _clean_rpr(tppr.find(W + "rPr"))
    ins = ctx.rev("ins")
    cap = _caption_rpr(template) if template is not None else None
    m = re.match(r"^([A-Z][^.:\n]{1,78}[.:])(\s+)(.+)$", text, re.S)
    if cap and m:
        ins.append(_text_run(m.group(1), cap[0]))
        ins.append(_text_run(m.group(2) + m.group(3), cap[1]))
    else:
        ins.append(_text_run(text, body_rpr))
    p.append(ins)
    return p


def _has_num(labels, i):
    return bool(labels[i]) and labels[i] != "\u2022"


def _template_for(paras, labels, levels, after, numbered):
    """The paragraph whose properties a new paragraph copies.

    numbered True: a new top-level clause, copying the last top-level
    numbered clause (never a sub-item or a heading's body). "same_level":
    another item at the anchor's own numbering level, e.g. (c) after (b).
    None: follow the anchor. False: unnumbered, laid out like the anchor."""
    if after < 0:
        return paras[0] if paras else None
    anchor = paras[after]
    if numbered in (False, None):
        return anchor
    numbered_before = [k for k in range(after, -1, -1) if _has_num(labels, k)]
    if numbered == "same_level":
        return paras[numbered_before[0]] if numbered_before else anchor
    top = [k for k in numbered_before if levels[k] == 0]
    if top:
        return paras[top[0]]
    if numbered_before:
        return paras[numbered_before[0]]
    return anchor


def _strip_numbering(p):
    ppr = p.find(W + "pPr")
    if ppr is not None:
        for el in ppr.findall(W + "numPr"):
            ppr.remove(el)


# ── Build + audit ────────────────────────────────────────────────────

def _schema_problems(body, author):
    problems = []
    ids = []
    for el in body.iter(W + "ins", W + "del", W + "moveFrom", W + "moveTo"):
        if el.get(W + "author") != author:
            problems.append(f"revision authored by {el.get(W + 'author')!r}")
        try:
            ids.append(int(el.get(W + "id")))
        except (TypeError, ValueError):
            problems.append("revision without a numeric id")
        holder = el.getparent()
        if holder is not None and holder.tag == W + "rPr" and \
                [c for c in holder if _is_tag(c)][0] is not el:
            problems.append("revision mark is not the first child of rPr")
    if len(ids) != len(set(ids)):
        problems.append("duplicate revision ids")
    if ids and min(ids) < ID_START:
        problems.append(f"revision ids start below {ID_START}")
    for d in body.iter(W + "del"):
        if any(True for _ in d.iter(W + "t")):
            problems.append("w:t inside a deletion (must be w:delText)")
            break
    for t in body.iter(W + "delText"):
        a = t.getparent()
        while a is not None and a.tag not in (W + "del", W + "moveFrom"):
            a = a.getparent()
        if a is None:
            problems.append("w:delText outside a deletion")
            break
    return problems


def build(source_bytes, plan, author=AUTHOR, when=None):
    """Build the redline and the clean copy from the counterparty's file.

    Returns (redline_bytes, clean_bytes, report). report["ok"] is True only
    when every audit passed; report["expected"] is NCP's position as
    paragraph texts."""
    from lxml import etree

    base_bytes = accept_all(source_bytes)
    doc = _load(base_bytes)
    paras = list_paragraphs(doc)
    info = numbering_info(doc, paras)
    labels = [lab for lab, _ in info]
    levels = [lvl for _, lvl in info]
    base_texts = [para_text(p) for p in paras]
    n = len(paras)
    date = when or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ctx = _Ctx(author, date)
    notes, applied = [], []
    expected = list(base_texts)
    changed = set()
    from lxml import etree as _et
    snapshot = [_et.tostring(p) for p in paras]
    # Anchors where new text must take over the paragraph mark (Word's
    # own model for pressing Enter at the end of a paragraph): ones that
    # end a section, a table cell or the document.
    ends_container = [p.find(W + "pPr/" + W + "sectPr") is not None
                      or _next_paragraph(p) is None for p in paras]

    for i, new in sorted(plan.get("modify", {}).items()):
        new = clean_text(new)
        if not 0 <= i < n:
            notes.append(f"¶{i} does not exist; edit skipped.")
            continue
        if norm_text(new) == norm_text(base_texts[i]):
            continue
        p = paras[i]
        old = para_text(p)
        ops, merged = diff_ops(old, new)
        _collapse_fields(p, ops)
        runs, segs_of, inserts, wraps = _apply_ops(p, old, ops, ctx)
        _place_insertions(p, old, new, runs, segs_of, inserts, ctx, wraps)
        expected[i] = merged
        changed.add(i)
        applied.append(("modify", i))

    for i in plan.get("delete", []):
        if not 0 <= i < n or i in changed:
            continue
        p = paras[i]
        _collapse_fields(p)
        old = para_text(p)
        _apply_ops(p, old, [("delete", 0, len(old), 0, 0)] if old else [],
                   ctx, delete_all=True)
        nxt = _next_paragraph(p)
        # A paragraph mark that carries a section break stays, or accepting
        # the deletion would take the page layout with it.
        holds_section = p.find(W + "pPr/" + W + "sectPr") is not None
        if nxt is not None and not holds_section:
            _mark_paragraph(p, ctx, "del")
            expected[i] = None
        else:
            expected[i] = ""
        changed.add(i)
        applied.append(("delete", i))

    inserted = {}
    last_new = {}
    for ins in plan.get("insert", []):
        after = ins.get("after", -1)
        if not -1 <= after < n:
            notes.append("A new clause had no valid position; skipped.")
            continue
        text = clean_text(ins.get("text") or "").strip()
        if not text:
            continue
        template = _template_for(paras, labels, levels, after,
                                 ins.get("numbered"))
        newp = _new_paragraph(template, text, ctx)
        if ins.get("numbered") is False:
            _strip_numbering(newp)
        if after in last_new:
            last_new[after].addnext(newp)
        elif after == -1:
            paras[0].addprevious(newp)
        else:
            paras[after].addnext(newp)
        last_new[after] = newp
        inserted.setdefault(after, []).append(text)
        applied.append(("insert", after))

    # After a paragraph that ends a section, a table cell or the document,
    # record the insertion the way Word records pressing Enter there: the
    # anchor's own paragraph mark is tracked as inserted, and the last new
    # paragraph inherits the original mark (and any section break), with
    # the anchor's old paragraph properties kept in a pPrChange. Rejecting
    # then restores the anchor, its section break and its layout exactly.
    for after, last in last_new.items():
        if after < 0 or not ends_container[after]:
            continue
        anchor = paras[after]
        if _mark(anchor) == "del":
            continue
        a_ppr = anchor.find(W + "pPr")
        l_ppr = last.find(W + "pPr")
        l_rpr = l_ppr.find(W + "rPr") if l_ppr is not None else None
        if l_rpr is not None:
            for m in l_rpr.findall(W + "ins"):
                l_rpr.remove(m)
        sect = a_ppr.find(W + "sectPr") if a_ppr is not None else None
        if sect is not None:
            a_ppr.remove(sect)
            l_ppr.append(sect)
        old_props = _el("pPr")
        if a_ppr is not None:
            for ch in a_ppr:
                if _is_tag(ch) and ch.tag not in (W + "rPr", W + "sectPr",
                                                  W + "pPrChange"):
                    old_props.append(copy.deepcopy(ch))
        new_props = [_et.tostring(c) for c in l_ppr if _is_tag(c) and c.tag
                     not in (W + "rPr", W + "sectPr", W + "pPrChange")]
        if new_props != [_et.tostring(c) for c in old_props]:
            change = ctx.rev("pPrChange")
            change.append(old_props)
            l_ppr.append(change)
        _mark_paragraph(anchor, ctx, "ins")
        changed.add(after)
        if sect is not None:
            notes.append(f"¶{after} ends a document section; the new text "
                         f"after it stays in that section.")

    exp = list(inserted.get(-1, []))
    for i in range(n):
        if expected[i] is not None:
            exp.append(expected[i])
        exp.extend(inserted.get(i, []))

    redline = _save(doc)

    # ── Audits (on freshly parsed files, never on in-memory state) ──
    audits, problems = {}, []
    rdoc = _load(redline)
    rparas = list_paragraphs(rdoc)
    rej = doc_view(rparas, "reject")
    acc = doc_view(rparas, "accept")
    audits["reject"] = rej == base_texts
    if not audits["reject"]:
        problems.append("Rejecting all changes does not reproduce the "
                        "counterparty's text.")
    audits["accept"] = acc == exp
    if not audits["accept"]:
        problems.append("Accepting all changes does not equal NCP's "
                        "position.")
    clean = accept_all(redline)
    cdoc = _load(clean)
    ctexts = [para_text(p) for p in list_paragraphs(cdoc)]
    leftover = story_revisions(clean)
    audits["clean"] = ctexts == exp and leftover == 0
    if not audits["clean"]:
        problems.append("The clean copy does not match NCP's position.")
    untouched_ok = True
    for i, p in enumerate(paras):
        if i not in changed and _et.tostring(p) != snapshot[i]:
            untouched_ok = False
            problems.append(f"¶{i} changed although NCP did not edit it.")
            break
    audits["untouched"] = untouched_ok
    schema = _schema_problems(rdoc.element.body, author)
    audits["schema"] = not schema
    problems.extend(sorted(set(schema)))
    audits["count"] = len(acc) == len(exp)

    dash = [t for t in [*(plan.get("modify", {}).values()),
                        *(x.get("text", "") for x in plan.get("insert", []))]
            if "—" in t or "–" in t]
    report = {
        "ok": all(audits.values()),
        "audits": audits,
        "problems": problems,
        "notes": notes,
        "applied": applied,
        "changes": ctx.next_id - ID_START,
        "author": author,
        "first_id": ID_START,
        "expected": exp,
        "baseline": base_texts,
        "style_warnings": (["NCP text contains an em or en dash; the "
                            "rulebook says to use commas, periods or "
                            "parentheses instead."] if dash else []),
    }
    return redline, clean, report


# ── Counter-round support ────────────────────────────────────────────

def _text_as(p, accept_authors):
    """Paragraph text with only the listed authors' revisions accepted and
    everyone else's rejected (None accepts every author)."""
    parts = []

    def ok(el):
        return accept_authors is None or el.get(W + "author") in accept_authors

    def walk(el):
        for ch in el:
            if not _is_tag(ch):
                continue
            if ch.tag == W + "r":
                parts.append(_run_text(ch))
            elif ch.tag in _INS:
                if ok(ch):
                    walk(ch)
            elif ch.tag in _DEL:
                if not ok(ch):
                    walk(ch)
            elif ch.tag in _CONTAINERS:
                walk(ch)
    walk(p)
    return "".join(parts)


def _revision_authors(el):
    return {x.get(W + "author") or "?"
            for x in el.iter(W + "ins", W + "del", W + "moveFrom", W + "moveTo",
                             W + "rPrChange", W + "pPrChange")}


def incoming_changes(upload_bytes, ncp_author=AUTHOR, limit=60):
    """The counterparty's live tracked changes, measured against NCP's own
    markup: NCP-authored revisions count as accepted, so a counterparty
    striking NCP language shows up as a change to NCP's text. Covers the
    body, headers, footers, footnotes, endnotes and comments, table rows,
    and formatting-only changes."""
    from lxml import etree
    import zipfile
    out = []
    ncp = {ncp_author}

    def scan(root, where):
        in_marked_row = set()
        for tr in root.iter(W + "tr"):
            if tr.find(W + "trPr/" + W + "del") is not None or \
                    tr.find(W + "trPr/" + W + "ins") is not None:
                in_marked_row.update(tr.iter(W + "p"))
        for p in root.iter(W + "p"):
            if (_in_textbox(p) and where == "body") or p in in_marked_row:
                continue
            theirs = _revision_authors(p) - ncp
            if not theirs:
                continue
            before, after = _text_as(p, ncp), _text_as(p, None)
            if before != after or _mark(p):
                out.append({"before": before, "after": after,
                            "authors": sorted(theirs), "mark": _mark(p),
                            "where": where})
            elif p.find(".//" + W + "rPrChange") is not None or \
                    p.find(".//" + W + "pPrChange") is not None:
                out.append({"before": before, "after": after,
                            "authors": sorted(theirs), "mark": None,
                            "where": where, "kind": "format"})
        for tr in root.iter(W + "tr"):
            mark = tr.find(W + "trPr/" + W + "del")
            kind = "row deleted"
            if mark is None:
                mark, kind = tr.find(W + "trPr/" + W + "ins"), "row inserted"
            if mark is not None and mark.get(W + "author") not in ncp:
                view_as = set() if kind == "row deleted" else None
                text = " | ".join(
                    " ".join(_text_as(cp, view_as) for cp in c.iter(W + "p"))
                    for c in tr.iter(W + "tc"))
                out.append({"before": text if kind == "row deleted" else "",
                            "after": text if kind == "row inserted" else "",
                            "authors": [mark.get(W + "author") or "?"],
                            "mark": None, "where": where, "kind": kind})

    doc = _load(upload_bytes)
    scan(doc.element.body, "body")
    with zipfile.ZipFile(io.BytesIO(upload_bytes)) as z:
        for name in z.namelist():
            m = re.match(r"^word/(header\d*|footer\d*|footnotes|endnotes|"
                         r"comments)\.xml$", name)
            if m:
                scan(etree.fromstring(z.read(name)), m.group(1))
    return out[:limit]


def untracked_changes(prev_texts, new_texts, limit=60):
    """Every paragraph-level difference between NCP's last position and the
    counterparty's new file, tracked or not, ignoring blank-line churn."""
    a = [norm_text(t) for t in prev_texts]
    b = [norm_text(t) for t in new_texts]
    sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
    out = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            continue
        entry = {"kind": tag,
                 "ncp_last": [prev_texts[k] for k in range(i1, i2)
                              if prev_texts[k].strip()],
                 "theirs": [new_texts[k] for k in range(j1, j2)
                            if new_texts[k].strip()],
                 "paras": list(range(j1, j2))}
        if entry["ncp_last"] or entry["theirs"]:
            out.append(entry)
    return out[:limit]


_PAGE_FURNITURE = re.compile(
    r"^(page\s+\d+(\s+of\s+\d+)?|\d{1,3}|-\s*\d+\s*-|\[?\d+\]?)$", re.I)
_CLAUSE_START = re.compile(
    r"^(\d+(\.\d+)*\.?|\(?[a-z]\)|\([ivxlc]+\)|[A-Z]\.|section\s+\d+|"
    r"article\s+[ivxlc\d]+)\s+[A-Z\u201c\"(]", re.I)
_FIELD_LINE = re.compile(r"^[A-Z][A-Za-z .&/-]{0,24}:(\s|$)")
# Clause captions at the start of a line: "Notices:" or a title of two or
# more words ending in a period ("Governing Law."). A lone "Company." is
# too often a mid-paragraph wrap to count.
_CAPTION_START = re.compile(
    r"^([A-Z][A-Za-z,&/-]*(\s+[A-Za-z,&/-]+){0,5}:\s|"
    r"[A-Z][a-z]+(\s+(of|and|the|to|on|for|[A-Z][a-z]+)){1,5}\.\s+[A-Z])")
_KEEP_HYPHEN = re.compile(r"(^|\W)(non|pre|post|co|self|anti|multi|sub|"
                          r"inter|cross|third|well|long|short)-$", re.I)


def _is_heading_line(s):
    """A short all-caps line (a title or clause heading). Long all-caps
    lines are wrapped disclaimer text and are joined as usual."""
    letters = [c for c in s if c.isalpha()]
    return (bool(letters) and s.upper() == s and len(s) <= 60
            and not s.endswith((",", ";")))


def _is_field_line(s):
    """A signature-style field line ("By: ____", "Name: Jane Doe"), as
    opposed to a clause whose caption ends in a colon."""
    return bool(_FIELD_LINE.match(s)) and len(s) <= 45


def docx_from_text(text):
    """A plain .docx rebuilt from extracted PDF text, so a PDF upload still
    gets a genuine tracked-changes redline (layout is not preserved).

    PDF text arrives as hard-wrapped lines. A line that finishes a
    sentence well short of the usual line width ends its paragraph; a
    full-width line is a wrap. Blank lines, headings, numbered clauses,
    closing and signature lines also start paragraphs. Page numbers and
    footers are dropped and hyphenated wraps are rejoined."""
    import docx
    lines = [l.strip() for l in clean_text(text or "").split("\n")]
    lines = [l for l in lines if not (l and _PAGE_FURNITURE.match(l))]
    widths = sorted(len(l) for l in lines if len(l) > 20)
    full = widths[int(len(widths) * 0.8)] if widths else 80
    paras, buf = [], []

    def flush():
        if buf:
            paras.append(" ".join(buf).replace("\u00ad ", ""))
            buf.clear()

    for s in lines:
        if not s:
            flush()
            continue
        if buf:
            prev = buf[-1]
            ended = bool(re.search(r"[.:;!?)\]\u201d\"]\s*$", prev))
            short = len(prev) < 0.85 * full
            caps = any(c.isalpha() for c in s) and s.upper() == s
            closing = _CLOSING_CUE.match(prev) and len(prev) <= 40
            if (_is_heading_line(prev) and len(buf) == 1) or closing \
                    or _is_field_line(prev) or _is_field_line(s) \
                    or (ended and (short or caps or _CLAUSE_START.match(s)
                                   or _CAPTION_START.match(s)
                                   or SIGNATURE_CUE.match(s)
                                   or _CLOSING_CUE.match(s))):
                flush()
            elif re.search(r"[A-Za-z]-$", prev) and s[:1].islower():
                if _KEEP_HYPHEN.search(prev):
                    buf[-1] = prev + "\u00ad"
                else:
                    buf[-1] = prev[:-1] + "\u00ad"
        buf.append(s)
    flush()
    d = docx.Document()
    for t in paras:
        d.add_paragraph(t.replace("\u00ad", ""))
    return _save(d)
