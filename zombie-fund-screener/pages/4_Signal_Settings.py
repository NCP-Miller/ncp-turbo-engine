"""Signal Settings — toggles, thresholds, weights, presets, cadence."""

import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs.settings import (get_signal_settings, save_signal_settings,
                          get_cadence, save_cadence, list_presets,
                          save_preset, delete_preset)

page_setup("Signal Settings — Zombie Fund Screener")
st.title("⚙️ Signal Settings")
st.caption("Every change re-scores instantly from stored data — nothing "
           "is re-fetched. Toggled-off signals leave both sides of the "
           "score so rankings stay comparable.")

settings = get_signal_settings()

# ── Presets ──────────────────────────────────────────────────────────
presets = list_presets()
p1, p2, p3 = st.columns([2, 2, 2])
with p1:
    if presets:
        chosen = st.selectbox("Load preset", ["—"] + list(presets))
        if chosen != "—" and st.button(f"Apply '{chosen}'"):
            save_signal_settings(presets[chosen])
            save_backup()
            st.rerun()
with p2:
    pname = st.text_input("Save current as preset",
                          placeholder="e.g., High conviction")
    if st.button("Save preset") and pname.strip():
        save_preset(pname.strip(), settings)
        save_backup()
        st.success(f"Saved '{pname.strip()}'.")
with p3:
    if presets:
        dsel = st.selectbox("Delete preset", ["—"] + list(presets))
        if dsel != "—" and st.button("Delete"):
            delete_preset(dsel)
            save_backup()
            st.rerun()

st.markdown("---")

# ── Per-signal controls ──────────────────────────────────────────────
changed = False


def _num(sid, tkey, label, minv, maxv, step=1.0):
    global changed
    cur = settings[sid]["thresholds"][tkey]
    new = st.slider(label, minv, maxv, type(minv)(cur), step=step,
                    key=f"{sid}_{tkey}")
    if new != cur:
        settings[sid]["thresholds"][tkey] = new
        changed = True


for sid, s in settings.items():
    with st.container(border=True):
        h1, h2, h3 = st.columns([4, 1, 2])
        h1.markdown(f"**{sid.upper()} — {s['name']}**")
        en = h2.toggle("On", value=s["enabled"], key=f"{sid}_en")
        if en != s["enabled"]:
            settings[sid]["enabled"] = en
            changed = True
        w = h3.slider("Weight", 0, 10, int(s["weight"]), key=f"{sid}_w")
        if w != s["weight"]:
            settings[sid]["weight"] = w
            changed = True

        if sid == "s1":
            _num(sid, "age_years", "Fund age threshold (years)", 7, 15)
        elif sid == "s2":
            c = st.columns(3)
            with c[0]:
                _num(sid, "raum_pct", "RAUM decline %", 10, 80)
                _num(sid, "raum_years", "over years", 1, 6)
            with c[1]:
                _num(sid, "emp_pct", "Employee decline %", 10, 90)
            with c[2]:
                _num(sid, "fund_age", "Oldest fund age (yrs)", 5, 20)
            sc = st.columns(3)
            for i, (tk, lbl) in enumerate(
                    [("use_raum", "Use RAUM test"),
                     ("use_emp", "Use employee test"),
                     ("use_age", "Use fund-age test")]):
                v = sc[i].checkbox(lbl, value=s["thresholds"][tk],
                                   key=f"{sid}_{tk}")
                if v != s["thresholds"][tk]:
                    settings[sid]["thresholds"][tk] = v
                    changed = True
        elif sid == "s3":
            _num(sid, "stale_years", "Stale after (years)", 1, 5)
        elif sid == "s4":
            _num(sid, "hold_years", "Held too long after (years)", 5, 12)
        elif sid == "s5":
            _num(sid, "decline_pct", "Headcount decline %", 20, 90)
            _num(sid, "hire_years", "No junior hire window (yrs)", 1, 6)
        elif sid == "s6":
            _num(sid, "vintage_max", "Vintage year (and earlier)", 2005, 2022)
            _num(sid, "dpi_max", "DPI below", 0.0, 1.5, step=0.05)
            _num(sid, "nav_floor_m", "Remaining NAV above ($M)", 1.0, 100.0,
                 step=1.0)
        elif sid == "s7":
            _num(sid, "exit_years", "No exit in (years)", 1, 5)
        elif sid == "s8":
            _num(sid, "stale_years", "Company site stale after (yrs)", 1, 5)
            logic = st.radio("Fire when", ["AND", "OR"], horizontal=True,
                             index=0 if s["thresholds"]["logic"] == "AND" else 1,
                             key=f"{sid}_logic",
                             help="AND: stale site AND a manual decay box. "
                                  "OR: either alone fires.")
            if logic != s["thresholds"]["logic"]:
                settings[sid]["thresholds"]["logic"] = logic
                changed = True
        elif sid == "s9":
            _num(sid, "window_years", "Change window (years)", 1, 6)
        elif sid == "s10":
            _num(sid, "window_years", "Filing window (years)", 1, 6)
            _num(sid, "amendment_max", "Amendments over", 1, 10)

if changed:
    save_signal_settings(settings)
    save_backup()
    st.toast("Settings saved — scores updated.")

st.markdown("---")

# ── Cadence + pool settings ──────────────────────────────────────────
st.markdown("#### Follow-up cadence & candidate pool")
cad = get_cadence()
cc = st.columns(5)
i1 = cc[0].number_input("Touch 1 → +bus. days", 1, 30, cad["intervals"][0])
i2 = cc[1].number_input("Touch 2 → +bus. days", 1, 30, cad["intervals"][1])
i3 = cc[2].number_input("Touch 3+ → +bus. days", 1, 30, cad["intervals"][2])
mx = cc[3].number_input("Max touches → Nurture", 1, 10, cad["max_touches"])
stale = cc[4].number_input("Stale after (days)", 5, 90, cad["stale_days"])
mins = st.number_input("Signals required to enter the candidate pool",
                       1, 10, cad.get("min_signals", 1))
new_cad = {"intervals": [i1, i2, i3], "max_touches": mx,
           "stale_days": stale, "min_signals": mins}
if new_cad != cad:
    save_cadence(new_cad)
    save_backup()
    st.toast("Cadence saved.")
