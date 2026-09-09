/* ══════════════════════════════════════════════════════════════════════════
   settings-boot.js — accessibility settings bootstrap

   applyUserSettings() lives in main.js, and main.js is only loaded by
   index.html.  So High contrast, the dyslexia font, larger text, reduce
   motion, underlined links, big touch targets and prominent focus rings all
   silently reverted the moment a user clicked any footer link (Terms,
   Privacy, Methodology, Learn, a /stocks/<ticker> page …).  For someone who
   needs those settings, the app appeared to forget them at random.

   This file re-applies the same localStorage → :root class mapping and must
   be loaded as a BLOCKING <script> in the <head> of every template, before
   first paint, so there is no flash of unstyled contrast.  Keep the storage
   key, defaults and palette identical to main.js.
   ══════════════════════════════════════════════════════════════════════════ */
(function () {
  "use strict";

  var SETTINGS_KEY = "valus.settings.v1";

  var DEFAULTS = {
    accent: "mint", textsize: "medium", lang: "en",
    contrast: false, motion: false,
    dyslexia: false, underline: false, touch: false, focusRing: false,
  };

  var ACCENT_PALETTE = {
    mint:   { accent: "#5eead4", soft: "rgba(94,234,212,0.12)",  glow: "rgba(94,234,212,0.25)",  strong: "rgba(94,234,212,0.55)" },
    blue:   { accent: "#60a5fa", soft: "rgba(96,165,250,0.12)",  glow: "rgba(96,165,250,0.25)",  strong: "rgba(96,165,250,0.55)" },
    purple: { accent: "#a78bfa", soft: "rgba(167,139,250,0.12)", glow: "rgba(167,139,250,0.25)", strong: "rgba(167,139,250,0.55)" },
    amber:  { accent: "#fbbf24", soft: "rgba(251,191,36,0.12)",  glow: "rgba(251,191,36,0.25)",  strong: "rgba(251,191,36,0.55)" },
    rose:   { accent: "#fb7185", soft: "rgba(251,113,133,0.12)", glow: "rgba(251,113,133,0.25)", strong: "rgba(251,113,133,0.55)" },
  };

  function read() {
    try {
      var raw = localStorage.getItem(SETTINGS_KEY);
      if (!raw) return DEFAULTS;
      var parsed = JSON.parse(raw) || {};
      var out = {};
      for (var k in DEFAULTS) out[k] = (k in parsed) ? parsed[k] : DEFAULTS[k];
      return out;
    } catch (e) { return DEFAULTS; }
  }

  function apply(s) {
    var root = document.documentElement;
    var palette = ACCENT_PALETTE[s.accent] || ACCENT_PALETTE.mint;
    root.style.setProperty("--accent",        palette.accent);
    root.style.setProperty("--accent-soft",   palette.soft);
    root.style.setProperty("--accent-glow",   palette.glow);
    root.style.setProperty("--accent-strong", palette.strong);
    root.classList.toggle("text-small", s.textsize === "small");
    root.classList.toggle("text-large", s.textsize === "large");
    root.classList.toggle("hc-on",    !!s.contrast);
    root.classList.toggle("rm-on",    !!s.motion);
    root.classList.toggle("dys-on",   !!s.dyslexia);
    root.classList.toggle("ul-on",    !!s.underline);
    root.classList.toggle("touch-on", !!s.touch);
    root.classList.toggle("focus-on", !!s.focusRing);
  }

  try { apply(read()); } catch (e) { /* never block rendering over a setting */ }

  // Exposed so main.js can re-run the same mapping after a settings change
  // without duplicating the table.
  window.__valusApplySettingsClasses = apply;
})();
