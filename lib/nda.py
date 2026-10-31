"""NDA Review engine: applies NCP's NDA rulebook to uploaded NDAs.

The rulebook (nda_rules.md, seeded from the NCP NDA Review Handoff and
editable in the app) drives a Claude Fable review that drafts NCP's
position paragraph by paragraph, accounts for every rulebook requirement
in a coverage checklist, buckets each change the NCP way, flags counsel
escalations, and is checked by a second senior-partner pass.

Deliverables per round come from lib/nda_redline.py, which edits the
counterparty's own .docx in place:
  - REDLINE: genuine Word tracked changes (author "New Capital Partners",
    IDs from 960), word-level, with untouched paragraphs left exactly as
    the counterparty sent them
  - CLEAN: the redline with every change accepted, ready to send
Every build is audited (reject-all reproduces their text, accept-all
equals NCP's position) before it is offered as send-ready.

Projects persist across rounds in pipeline_data/nda_review.db, named by
deal project so iterations can be reviewed until the NDA is executable,
and mirror to the GitHub data branch.
"""

import io
import json
import os
import sqlite3
from datetime import datetime, timezone

from lib import nda_redline as R

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DB_DIR = os.path.join(_ROOT, "pipeline_data")
_DB = os.path.join(_DB_DIR, "nda_review.db")
_RULES_FILE = os.path.join(_ROOT, "nda_rules.md")

DEAL_TYPES = [
    "Add-on for a portfolio company",
    "New platform — potentially competitive",
    "New platform — non-competitive",
    "Not sure yet — apply the full protection set",
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


# ── Settings ──────────────────────────────────────────────────────────

def _meta_get(key, default=None):
    init_db()
    conn = _connect()
    try:
        r = conn.execute("SELECT value FROM nda_meta WHERE key = ?",
                         (key,)).fetchone()
        return r["value"] if r and r["value"] is not None else default
    finally:
        conn.close()


def _meta_set(key, value):
    init_db()
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO nda_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value))
        conn.commit()
    finally:
        conn.close()


SIGNING_DEFAULTS = {
    "entity": "NCP Management Holdings, LLC",
    "address": "2101 Highland Ave S, Suite 700, Birmingham, AL 35205",
    "signatory": 'William "Trey" Miller III',
    "title": "Managing Director and Authorized Signor",
    "email": "tmiller@newcapitalpartners.com",
    "phone": "",
}


def get_signing():
    try:
        saved = json.loads(_meta_get("signing", "{}") or "{}")
    except json.JSONDecodeError:
        saved = {}
    return {**SIGNING_DEFAULTS, **{k: v for k, v in saved.items()
                                   if k in SIGNING_DEFAULTS}}


def set_signing(details):
    _meta_set("signing", json.dumps(
        {k: (details.get(k) or "").strip() for k in SIGNING_DEFAULTS}))


# Claude Fable is the negotiation brain; Opus and Sonnet are access
# fallbacks; GPT-4o is the last resort.
ANTHROPIC_MODELS = ["claude-fable-5-1", "claude-opus-5-5", "claude-sonnet-5-5"]
EFFORT_LEVELS = ["high", "xhigh", "max", "medium"]


def get_review_model():
    m = _meta_get("model")
    return m if m in ANTHROPIC_MODELS else ANTHROPIC_MODELS[0]


def set_review_model(model):
    _meta_set("model", model)


def get_effort():
    e = _meta_get("effort")
    return e if e in EFFORT_LEVELS else EFFORT_LEVELS[0]


def set_effort(effort):
    _meta_set("effort", effort)


def get_self_audit():
    return _meta_get("self_audit", "1") != "0"


def set_self_audit(on):
    _meta_set("self_audit", "1" if on else "0")


# ── Upload handling ───────────────────────────────────────────────────

def _pdf_text(file_bytes):
    try:
        from pypdf import PdfReader
    except Exception as e:
        raise RuntimeError(f"PDF support unavailable on this server ({e}). "
                           f"Upload the .docx instead.")
    reader = PdfReader(io.BytesIO(file_bytes))
    return "\n".join((page.extract_text() or "") for page in reader.pages)


def prepare_upload(file_bytes, filename):
    """The .docx the redline is built on, and whether it was rebuilt from a
    PDF (layout lost, but the redline is still genuine tracked changes)."""
    name = (filename or "").lower()
    if name.endswith(".docx"):
        return file_bytes, False
    if name.endswith(".pdf"):
        return R.docx_from_text(_pdf_text(file_bytes)), True
    raise RuntimeError("Upload a .docx or .pdf file.")


def deliverable_name(original_filename, suffix):
    """Output filename built from the UPLOADED file's name, so the
    banker's naming survives: 'BizPort NDA.docx' -> 'BizPort NDA - NCP
    Redline.docx'."""
    stem = os.path.splitext(os.path.basename(original_filename or "NDA"))[0]
    return f"{stem} - {suffix}.docx"


def count_tracked_changes(docx_bytes):
    """Incoming-file check: how many live w:ins / w:del marks?"""
    try:
        return R.count_revisions(docx_bytes)
    except Exception:
        return 0, 0


# ── The review brain ─────────────────────────────────────────────────

COVERAGE_CHECKLIST = [
    ("competing_investments", "Standalone Competing Investments / Nature of "
     "Business clause (its own clause): NCP evaluates, makes and manages "
     "investments across many companies and industries; NCP and its "
     "Affiliates may invest in competitors; nothing in the NDA limits that, "
     "provided Confidential Information is not used or disclosed in breach"),
    ("residuals", "Residuals backup inside the Competing Investments clause: "
     "the opening position is unaided-memory residuals plus the industry-"
     "knowledge acknowledgment (usable for internal purposes, including "
     "other investment decisions); the Copa industry-knowledge-only wording "
     "is the fallback, not the opener"),
    ("ci_survival_supremacy", "Competing Investments (and No Imputed "
     "Knowledge) survive indefinitely and control over any contrary current "
     "or future agreement of the Recipient or its Representatives, yielding "
     "only to a writing signed by the Recipient that expressly references the "
     "clause (Black Rose)"),
    ("dual_hat", "Dual Hat Employees on every deal: portfolio companies are "
     "not deemed to receive Confidential Information, or to be restricted, "
     "because NCP personnel sit on their boards or serve as officers or "
     "advisors, provided the dual-hat person does not pass Confidential "
     "Information to portfolio company personnel"),
    ("no_imputed_knowledge", "No imputed knowledge: knowledge limited to "
     "individuals who actually receive information; portfolio companies are "
     "not Affiliates or Representatives unless they actually receive it"),
    ("data_room_supremacy", "The NDA governs over any current or future data "
     "room, click-through or similar terms"),
    ("compulsion_cost", "Legal compulsion: any cooperation is at the "
     "disclosing party's sole cost"),
    ("compulsion_counsel", "Legal compulsion: Recipient's counsel (internal "
     "counsel acceptable) decides the scope of required disclosure"),
    ("compulsion_regulatory", "Legal compulsion: regulatory and bank-examiner "
     "requests carved out with no notice required"),
    ("representatives", "Representatives limited to those who actually "
     "receive information; lenders and financing sources included; "
     "co-investors included; Affiliates only on add-on deals"),
    ("rep_responsibility", "NCP not responsible for a Representative's breach "
     "where that Representative signs a joinder or separate NDA"),
    ("return_destroy", "Return or destruction at NCP's election, email "
     "confirmation of destruction sufficient, retention for legal, "
     "regulatory, compliance and IT/archival backup purposes"),
    ("term", "Term per the ladder (1 year preferred; counter 2 years at 18 "
     "months; counter 3 to 5 years at 2 years) and ONE sunset governing every "
     "obligation (non-disclosure, use restriction, retained copies), except "
     "the clauses meant to survive; no 'for as long as it is retained' tail"),
    ("non_solicit", "Non-solicit: 1 year; management-level employees with "
     "direct contact in the Transaction; general advertising carve-out; binds "
     "NCP only, not portfolio companies"),
    ("contact_restrictions", "Contact restrictions limited to contacts "
     "concerning the proposed Transaction, with a carve-out for general market "
     "research and customary diligence that does not reveal Confidential "
     "Information, its receipt, or Transaction terms"),
    ("carve_outs", "Confidentiality carve-outs broad (public, already known, "
     "independently developed, received from a third party) with the "
     "knowledge qualifier 'after all reasonable inquiry'"),
    ("no_obligation", "No obligation to proceed; nothing binds until a "
     "definitive written agreement is signed; either side may walk away"),
    ("governing_law", "Delaware law with exclusive Court of Chancery forum"),
    ("remedies", "Injunctive relief: the disclosing party 'may seek' it, never "
     "an automatic entitlement; no waiver of the actual-damages requirement; "
     "no waiver of bond"),
    ("liability", "Liability capped at direct damages only, as determined by "
     "final court order (fraud/willful-misconduct carve-outs are a Copa "
     "fallback, not an opening position)"),
    ("no_noncompete_standstill", "No non-compete, no standstill, and no "
     "no-contact or non-solicit broad enough to work as one"),
    ("party_details", "NCP party details and signature fields filled per the "
     "signing table; Recipient defined as the NCP entity only; By: line left "
     "blank; nothing left as a placeholder"),
    ("countersignature", "One-way letter signed only by NCP: a Company "
     "acceptance block (banker signing as authorized agent) so the Company "
     "is bound by the acknowledgments NCP added; not_applicable on forms "
     "both sides sign"),
    ("defined_terms", "One label for NCP in every operative clause (the "
     "Recipient), every capitalized term defined, references to information "
     "use the defined Confidential Information, and no undefined 'the "
     "Company' or 'transaction(s) contemplated by this Agreement'"),
    ("counsel_flags", "Counsel red flags surfaced: liquidated damages, "
     "indemnification, exclusivity, anything binding NCP's LPs"),
]

_COVERAGE_TEXT = "\n".join(f"- {k}: {t}" for k, t in COVERAGE_CHECKLIST)

_OUTPUT_SCHEMA = """{
  "summary": "report to Trey, 3 to 6 sentences: what matters, what is weak, what to watch",
  "executable": false,
  "executable_reasoning": "one sentence",
  "net_discloser_note": "mutual forms only: who is the net discloser and what that means; else empty",
  "escalations": ["counsel red flags and judgment calls for Trey, each citing the paragraph"],
  "accepted_as_drafted": ["provisions left alone because they already meet NCP's standard, each with the paragraph"],
  "counterparty_moves": [{"para": 7, "what_they_did": "their change since NCP's last turn", "ncp_response": "accept|reject|counter", "note": "why; flag any silent acceptance"}],
  "edits": [
    {"action": "modify", "para": 6, "text": "the complete revised paragraph",
     "changes": [{"bucket": "non_negotiable|trade|style", "title": "short label, e.g. Term 2 years to 18 months", "rationale": "why, citing the rulebook"}]},
    {"action": "insert_after", "para": 15, "numbered": true, "text": "Caption. Full text of the new clause.",
     "changes": [{"bucket": "non_negotiable", "title": "...", "rationale": "..."}]},
    {"action": "delete", "para": 9, "changes": [{"bucket": "...", "title": "...", "rationale": "..."}]}
  ],
  "coverage": [{"item": "checklist key", "status": "in_form|added|revised|not_applicable|conceded|missing", "paras": [6], "note": "short"}],
  "fallbacks_available": ["ladder or precedent fallbacks still in reserve for later rounds"],
  "fallbacks_used": [{"fallback": "ladder row or precedent used in this draft", "instruction": "where Trey instructed it (round and wording)"}],
  "cover_email": "brief cover email to the banker or counsel describing each change in a sentence; no em dashes",
  "audit_notes": []
}"""


def _system_prompt(rules):
    return f"""You are the NDA negotiator for New Capital Partners (NCP), a private equity firm that is usually the buyer and the Recipient of confidential information. Trey Miller, NCP's Managing Director, sends your work straight back to the counterparty's banker or counsel, so it has to be right on substance under NCP's rulebook and drafted the way an experienced M&A lawyer marks up someone else's form.

HOW YOUR EDITS BECOME THE REDLINE
The app turns your edits into genuine Word tracked changes inside the counterparty's own document. For each paragraph you revise it compares your text with the original word by word and marks only the words that differ. That shapes how you draft:
- Revise a paragraph by returning its complete new text in one "modify" edit, with every change to that paragraph in that one edit. Copy each word you keep exactly as it appears (capitalization, punctuation, defined terms); anything you retype differently shows up as a change.
- Keep the counterparty's language wherever it works and change only the words that need to change. A banker accepts a surgical markup much faster than a rewrite. Replace a clause wholesale only when targeted edits cannot fix its structure.
- A sentence that belongs to an existing clause (a carve-out, a qualifier, an avoidance-of-doubt sentence, the counsel-decides and regulator carve-outs) goes inside that clause's paragraph, never in a paragraph of its own.
- New standalone clauses use "insert_after" with the paragraph number of the last operative numbered clause (the document view marks where the signature block starts; never put a clause after that point) and "numbered": true. They join the document's numbering automatically, so if the last clause is auto-numbered 6, your new clauses become 7, 8 and so on in the order you list them; use those numbers in cross-references. "numbered": true always makes a new top-level clause, even when anchored after a sub-item; use "numbered": "same_level" only to add another item at the anchor's own level (for example a (c) after (b)), and false for unnumbered text. Open each with a short caption such as "Competing Investments; Nature of Business."
- Fill NCP's details into the form's existing blanks and signature fields in place by modifying those paragraphs. Never add a second NCP signature block. Leave the By: line and the date blank. (The Company acceptance block for one-way letters is covered under the drafting standards below.)
- The Recipient is the NCP entity in the signing details. Trey signs only as its authorized signatory; never make him, or "the undersigned" as an individual, a party or part of the Recipient definition.
- Use the form's existing defined terms consistently, define any new term before relying on it, and make sure every term the final text uses (Confidential Information, Representatives, Effective Date, Transaction, Purpose and so on) is defined somewhere.
- Match the document's quote style. Never use em dashes or en dashes in drafted text; use commas, periods or parentheses.
- Open at NCP's standard positions. Concession-ladder and precedent (Copa, Mariner) fallbacks are only for later rounds and only on Trey's instruction; never volunteer one in a draft. List the ones still in reserve in "fallbacks_available". Use a fallback only where the prior rounds record Trey instructing it, and then list it in "fallbacks_used" with where he did; otherwise "fallbacks_used" stays empty.

NCP DRAFTING STANDARDS (lessons from past markups; they apply alongside the rulebook)
- Party line: "The undersigned, [signatory], [title], as duly authorized agent for [entity] (the "Recipient")". Then use "the Recipient" as the obligor in every operative clause, converting "the undersigned" with word-level swaps, and fix undefined labels such as "the Company" or "this Company".
- Term: one sunset governs every obligation. Add it to the term clause ("at which time all obligations under this Agreement terminate, except Paragraph N, which survives indefinitely") and make retained copies subject to confidentiality only until the term ends. Treat "for as long as it is maintained/retained" as a tail.
- Representatives: the "who actually receive Confidential Information" limiter belongs inside the definition itself. Include lenders, other financing sources and co-investors; affiliates only on add-on deals; "partners" should exclude limited partners in their capacity as such. Do not add an affirmative undertaking that the Recipient is responsible for its Representatives when the form has none; state only a "will direct its Representatives" duty and the joinder or separate-NDA safe harbor.
- Return or destruction: destruction or return at the Recipient's election, retention carve-outs for legal, regulatory, compliance and backup systems. Say email confirmation is sufficient only where the form already requires a confirmation or certification; never add a new confirmation duty.
- Non-solicit and no-contact: "directly solicit for employment" management-level employees with direct contact in the Transaction; the carve-out verbs must match the restriction ("soliciting or hiring"); the restriction binds the Recipient only, not affiliates or portfolio companies (say so inside that clause). Put the Transaction qualifier on the contact itself: "knowingly contact, concerning the proposed Transaction, any customer, supplier or other business relation of the Subject Company identified as such in the Confidential Information", with a carve-out for ordinary-course dealings and customary market research or diligence that does not reveal Confidential Information, its receipt, or Transaction terms. Define "Transaction" if the form does not.
- Acknowledgments that information is "confidential and proprietary" or usable "solely" for a purpose should refer to the defined Confidential Information and the defined Purpose so they do not override the exclusions or the Competing Investments clause.
- Remedies: "may seek" injunctive relief for an actual breach (not "threatened"); state that nothing waives the requirement to prove actual damages or to post a bond; liability limited to direct damages as determined by final, non-appealable court order, with consequential, incidental, indirect, special, exemplary and punitive damages and lost profits excluded.
- Supremacy language (Competing Investments, data room terms) reaches agreements and click-throughs of the Recipient and its Representatives, current or future, and yields only to a writing signed by the Recipient that expressly references the clause.
- One-way letter signed only by NCP: when your edits add acknowledgments or obligations of the Company, add a short acceptance block so the Company is bound: one insert_after with "numbered": false, placed right after NCP's signature fields and before any delivery instructions, lines separated by ⏎, e.g. "ACKNOWLEDGED AND AGREED on behalf of the Subject Company:⏎[banker], as authorized agent for the Subject Company⏎By: ____________________⏎Name:⏎Title:⏎Dated:". Tell Trey in the summary that he can drop it and ask by email instead.
- Watch for, and raise in escalations, anything the rulebook does not settle: assignment of the Company's rights (for example the non-solicit passing to a strategic buyer), third-party beneficiaries, and any language that could bind NCP's LPs. When two rulebook rules pull in opposite directions for NCP's role, follow the more specific one and flag the tension.

COVERAGE
Before you finish, read the document as it will stand after your edits and account for every item below in "coverage": "in_form" when the counterparty's text already meets NCP's standard, "added" or "revised" when your edits deliver it (cite the paragraphs), "not_applicable" with a reason, "conceded" only where Trey has instructed it in this negotiation, "missing" only if you could not deliver it (explain why). Deliver every item that applies; a coverage claim must be true of the final text.
{_COVERAGE_TEXT}

REPORTING TO TREY
Short, direct and honest about weak spots. Bucket each change as the rulebook does: "non_negotiable", "trade" (credibility trades; name the fallback being traded) or "style" (conforming and clean-up). List what you left alone in "accepted_as_drafted" so nothing is accepted silently. On counter-rounds, account for every counterparty change in "counterparty_moves", and call out any silent acceptance, including a counterparty deleting something NCP inserted. Explain direction of benefit when a counterparty change actually helps NCP. Put counsel red flags and judgment calls in "escalations". If the form is a one-way letter signed only by NCP, explain in the summary that the Company acceptance block is there so the Company-side acknowledgments (Competing Investments, supremacy, remedies) bind the Company.

THE NCP NDA RULEBOOK (the authority; where it and general practice disagree, the rulebook wins)
{rules}

OUTPUT
Return one JSON object and nothing else, in this shape:
{_OUTPUT_SCHEMA}"""


_GATE_NOTES = {
    "Add-on for a portfolio company": (
        "Competitive overlap exists by definition (the target operates in an "
        "NCP portfolio company's space). Apply the full protection set: "
        "portfolio company non-imputation, non-solicit carve-out for "
        "portfolio companies, Dual Hat Employees, hardened Competing "
        "Investments, and Affiliates in Representatives per the ladder."),
    "New platform — potentially competitive": (
        "Apply the full protection set (portfolio company non-imputation, "
        "non-solicit carve-out for portfolio companies, Dual Hat Employees, "
        "hardened Competing Investments). Affiliates stay out of "
        "Representatives."),
    "New platform — non-competitive": (
        "Standard protection set: no portfolio-company carve-outs beyond the "
        "rulebook's every-deal items. The standalone Competing Investments "
        "clause with survival and supremacy and the Black Rose additions "
        "(Dual Hat, data room supremacy, legal-compulsion refinements) still "
        "apply on every deal."),
    "Not sure yet — apply the full protection set": (
        "Trey has not confirmed whether the target could compete with an NCP "
        "business or portfolio company. Apply the full protection set (as for "
        "a potentially competitive platform) and open the summary with a "
        "one-line request that Trey confirm Gate 2 before this goes out."),
}


def _user_prompt(document_view, deal_type, ncp_role, form_source, signing,
                 prior_context, counterparty_changes):
    sig = "\n".join(f"- {k}: {v}" for k, v in signing.items() if v)
    history = (f"PRIOR ROUNDS OF THIS NEGOTIATION\n{prior_context}\n\n"
               if prior_context else "This is Round 1 of this negotiation.\n\n")
    theirs = (f"COUNTERPARTY CHANGES SINCE NCP'S LAST TURN\n"
              f"{counterparty_changes}\n\n" if counterparty_changes else "")
    return f"""DEAL GATES (answered by Trey)
- Deal type: {deal_type.replace(" — ", ": ")}. {_GATE_NOTES.get(deal_type, "")}
- NCP role: {ncp_role}. As Recipient, carve-outs stay broad and Disclosing-Party moves never apply. On a mutual form, identify the net discloser.
- Form source: {form_source}. Banker forms tend to carry standstills, broad no-contact lists and tails.

NCP SIGNING DETAILS
{sig}

{history}{theirs}THE DOCUMENT
Each line is one paragraph of the counterparty's document, tagged with its paragraph number (use it as "para" in edits) and its auto-number when it has one. Auto-numbers are generated by Word and are not part of the paragraph text; do not type them into your revised text. A ⏎ marks a line break inside a paragraph; keep it where it belongs. Empty paragraphs are omitted.

{document_view}"""


_AUDIT_INSTRUCTION = """An NCP associate drafted the review below for this NDA. You are the senior partner checking it before it reaches Trey. Re-read the document and the rulebook, then fix anything wrong or missing:
- every coverage item is actually delivered by the final text (read the text, not the claim), or is honestly in_form or not_applicable;
- no Disclosing-Party moves, no volunteered fallbacks, and no non-compete, standstill or tail left standing;
- each modify edit keeps the counterparty's wording wherever it works, reproduces unchanged words exactly, and carries every change for that paragraph;
- defined terms, cross-references, the numbering of new clauses, placement before the signature block, and signature fields filled in place are all correct;
- nothing in the draft leaves NCP worse off than the counterparty's form.
Return the complete corrected review JSON in the same shape (the full object, not a diff). In "audit_notes" list each thing you changed and why, or leave it empty if the draft was right.

DRAFT REVIEW TO CHECK
"""


def _extract_json(text):
    import re
    t = (text or "").strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t)
    start, end = t.find("{"), t.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("The model returned no JSON.")
    body = t[start:end + 1]
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return json.loads(re.sub(r",\s*([}\]])", r"\1", body))


def _claude_call(client, model, effort, system, user):
    """One streamed Claude call. Fable and Opus 5.5 think adaptively on
    their own; effort sets how hard. Server-side fallback reroutes a
    policy decline to Anthropic's recommended model."""
    import anthropic
    kwargs = dict(
        model=model,
        max_tokens=64000,
        system=[{"type": "text", "text": system,
                 "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": user}],
        output_config={"effort": effort},
    )
    try:
        with client.beta.messages.stream(
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default", **kwargs) as s:
            resp = s.get_final_message()
    except anthropic.BadRequestError as e:
        if "fallback" not in str(e).lower():
            raise
        with client.messages.stream(**kwargs) as s:
            resp = s.get_final_message()
    if resp.stop_reason == "refusal":
        raise RuntimeError(f"{model} declined the request")
    if resp.stop_reason == "max_tokens":
        raise RuntimeError(f"{model} ran out of output room")
    text = "".join(b.text for b in resp.content
                   if getattr(b, "type", "") == "text")
    return _extract_json(text), getattr(resp, "model", model) or model


def review_nda(document_view, deal_type, ncp_role, form_source, rules,
               prior_context="", counterparty_changes="", signing=None,
               anthropic_client=None, anthropic_model=None,
               openai_client=None, effort=None, self_audit=None,
               progress=None, view=None):
    """One review round over the paragraph-tagged document view.

    Returns (review_dict, engine_label). The review's edits are keyed to
    paragraph numbers and feed the redline engine directly. With the
    baseline `view`, the self-audit pass reads the document as it would
    stand with the draft accepted, not just the list of edits."""
    say = progress or (lambda msg: None)
    signing = signing or get_signing()
    effort = effort or get_effort()
    self_audit = get_self_audit() if self_audit is None else self_audit
    system = _system_prompt(rules)
    user = _user_prompt(document_view, deal_type, ncp_role, form_source,
                        signing, prior_context, counterparty_changes)

    if anthropic_client is not None:
        import anthropic
        first = anthropic_model or get_review_model()
        order = [first] + [m for m in ANTHROPIC_MODELS if m != first]
        last_err = None
        for model in order:
            try:
                say(f"Drafting NCP's position with {model} "
                    f"({effort} effort)...")
                review, served = _claude_call(anthropic_client, model, effort,
                                              system, user)
            except (anthropic.AuthenticationError,
                    anthropic.PermissionDeniedError) as e:
                last_err = e
                break
            except Exception as e:
                last_err = e
                continue
            label = f"Claude {served} · adaptive thinking, {effort} effort"
            if served != model:
                label += f" (served by fallback from {model})"
            if self_audit:
                say("Senior-partner pass: auditing the draft against the "
                    "rulebook...")
                final = ""
                if view is not None:
                    try:
                        plan, _ = R.plan_from_edits(review.get("edits", []),
                                                    view)
                        final = ("\n\nTHE DOCUMENT AS IT WOULD READ WITH THE "
                                 "DRAFT ACCEPTED (judge this text, not the "
                                 "claims)\n" + R.final_view(view, plan))
                    except Exception:
                        final = ""
                try:
                    audited, _ = _claude_call(
                        anthropic_client, served, effort, system,
                        user + "\n\n" + _AUDIT_INSTRUCTION
                        + json.dumps(review, ensure_ascii=False) + final)
                    if isinstance(audited.get("edits"), list):
                        if review.get("edits") and not audited["edits"]:
                            audited.setdefault("audit_notes", []).append(
                                "The senior-partner pass withdrew every edit "
                                "in the first draft; check its summary.")
                        review = audited
                        label += " · self-audited"
                    else:
                        review["_audit_error"] = ("the senior-partner pass "
                                                  "returned no edit list")
                except Exception as e:
                    review["_audit_error"] = f"{type(e).__name__}: {e}"[:300]
            review["format"] = 2
            return review, label
        if openai_client is None:
            raise RuntimeError(f"Claude review failed: {last_err}")
        fallback_note = (f"GPT-4o (fallback — Claude failed: "
                         f"{type(last_err).__name__}: {str(last_err)[:300]})")
    else:
        fallback_note = "GPT-4o (no ANTHROPIC_API_KEY configured)"

    if openai_client is None:
        raise RuntimeError(
            "No AI configured. Add ANTHROPIC_API_KEY (preferred — enables "
            "Claude) or OPENAI_API_KEY to secrets.")
    say("Drafting with GPT-4o (fallback)...")
    resp = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        response_format={"type": "json_object"},
        temperature=0.2,
        timeout=240,
    )
    review = json.loads(resp.choices[0].message.content)
    review["format"] = 2
    return review, fallback_note


# ── Deliverables ──────────────────────────────────────────────────────

def plan_for(review, view):
    """The redline plan for a stored review: paragraph-keyed edits for
    reviews from this version on, substring edits for older rounds."""
    if review.get("format") == 2:
        plan, notes = R.plan_from_edits(review.get("edits", []), view)
        return plan, notes, []
    plan, skipped = R.plan_from_legacy(review.get("edits", []), view)
    return plan, [], skipped


def build_deliverables(source_bytes, review):
    """(redline_bytes, clean_bytes, report) for one round. The report
    carries the audits; report['ok'] gates whether the files are
    send-ready."""
    view = R.baseline_view(source_bytes)
    plan, notes, skipped = plan_for(review, view)
    redline, clean, report = R.build(source_bytes, plan)
    report["notes"] = notes + report["notes"]
    report["skipped"] = skipped
    report["plan"] = plan
    report["labels"] = view["labels"]
    return redline, clean, report


def format_counterparty_changes(tracked=None, untracked=None):
    """Text block describing what the counterparty changed, for the
    reviewer's prompt. Tracked changes are measured against NCP's own
    markup, so striking NCP language shows as a change to NCP's text."""
    lines = []
    if tracked:
        lines.append("Their tracked changes (measured against NCP's last "
                     "markup):")
    for c in tracked or []:
        who = ", ".join(c.get("authors") or [])
        where = "" if c.get("where", "body") == "body" else \
            f" in {c['where']}"
        kind = c.get("kind")
        if kind == "format":
            lines.append(f"- Formatting-only change{where} ({who}): "
                         f"{c['after'][:200]}")
        elif kind in ("row deleted", "row inserted"):
            lines.append(f"- Table {kind}{where} ({who}): "
                         f"{c['before'] or c['after']}")
        elif c.get("mark") == "ins" and not c.get("before"):
            lines.append(f"- Inserted{where} ({who}): {c['after']}")
        elif c.get("mark") == "del" and not c.get("after"):
            lines.append(f"- Deleted{where} ({who}): {c['before']}")
        else:
            lines.append(f"- Changed{where} ({who}):\n  NCP's markup: "
                         f"{c['before']}\n  now: {c['after']}")
    if untracked:
        lines.append("Every difference between NCP's last position and "
                     "their file once all changes are accepted (catches "
                     "edits they made without Track Changes):")
    for u in untracked or []:
        paras = ", ".join(f"¶{p}" for p in u.get("paras", [])) or "removed"
        was = " / ".join(u.get("ncp_last") or []) or "(nothing)"
        now = " / ".join(u.get("theirs") or []) or "(deleted)"
        lines.append(f"- At {paras}:\n  NCP's last position: {was}\n"
                     f"  their file now: {now}")
    return "\n".join(lines)


# ── Project / round storage ──────────────────────────────────────────

def create_project(name, deal_type, ncp_role, form_source):
    init_db()
    conn = _connect()
    try:
        existing = conn.execute(
            "SELECT id FROM projects WHERE lower(trim(name)) = lower(trim(?))",
            (name,)).fetchone()
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


def delete_project(project_id):
    """Remove a project and all its rounds — the 'start fresh at Round
    1' path. The next backup overwrites nda_review.json, so the delete
    sticks across redeploys."""
    conn = _connect()
    try:
        conn.execute("DELETE FROM rounds WHERE project_id = ?",
                     (project_id,))
        conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
        conn.commit()
    finally:
        conn.close()


def delete_round(round_id):
    """Drop a single round (e.g. a bad run) without losing the project."""
    conn = _connect()
    try:
        r = conn.execute("SELECT project_id FROM rounds WHERE id = ?",
                         (round_id,)).fetchone()
        conn.execute("DELETE FROM rounds WHERE id = ?", (round_id,))
        if r:
            conn.execute("UPDATE projects SET updated_at = ? WHERE id = ?",
                         (_now(), r["project_id"]))
        conn.commit()
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


def _change_titles(review):
    titles = []
    for e in review.get("edits", []):
        if review.get("format") == 2:
            for c in e.get("changes") or []:
                if c.get("title"):
                    titles.append(f"{c['title']} [{c.get('bucket', '')}]")
        elif e.get("rationale"):
            titles.append(e["rationale"][:120])
    return titles


def prior_context(project_id):
    """History of earlier rounds for the next review prompt: what NCP
    asked for each round, so a counter-round can hold or trade knowingly."""
    lines = []
    for r in list_rounds(project_id):
        rv = r.get("review", {})
        lines.append(f"Round {r['round_no']} ({(r['created_at'] or '')[:10]}, "
                     f"file {r.get('filename') or 'unknown'}): "
                     f"{rv.get('summary', 'no summary')} "
                     f"Executable: {rv.get('executable')}")
        titles = _change_titles(rv)
        if titles:
            lines.append("  NCP's changes that round: " + "; ".join(titles))
        for m in rv.get("counterparty_moves") or []:
            lines.append(f"  Counterparty move: {m.get('what_they_did', '')} "
                         f"-> NCP {m.get('ncp_response', '')}")
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
