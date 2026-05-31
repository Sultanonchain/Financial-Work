# VALUS — Security Notes & Go-Live Checklist

_Last reviewed: 2026-05-31_

This file documents the security posture of VALUS and the steps required
before accepting real payments. Keep it updated as the app changes.

## Security review summary

A full audit was performed on the codebase and re-run after the account /
access-code features were added. **No outstanding high or critical issues.**

### Controls in place
- **Secrets**: never hardcoded; all via environment variables. `.env` is
  gitignored and absent from git history.
- **Authentication**: Google OAuth via Authlib (state/CSRF handled by the
  library). Open-redirect blocked on the auth callback.
- **Authorization**: all per-user routes (portfolio, watchlist, leaderboard,
  subscription, account-delete) key on the session-derived `user["sub"]` —
  no IDOR.
- **Sessions**: signed cookies (`SECRET_KEY`), `HttpOnly`, `SameSite=Lax`,
  `Secure` in production. The `valus_unlimited` access flag rides in the
  signed cookie and cannot be forged client-side.
- **CORS**: restricted to an allow-list (`ALLOWED_ORIGINS`, default = prod
  domains + localhost). No wildcard-with-credentials.
- **Rate limiting**: flask-limiter on all data routes; `/api/redeem-code`
  is capped at 10/min to prevent code brute-forcing; analyze is capped at
  40/min per signed-in user even with a team code (no cost-abuse vector).
- **Secrets comparison**: `CRON_SECRET`, `/api/_diag/kv`, and access codes
  use `hmac.compare_digest` (constant-time).
- **Stripe webhook**: fails closed — rejects unsigned events unless
  `STRIPE_WEBHOOK_SECRET` is set (dev-only opt-out via
  `STRIPE_WEBHOOK_ALLOW_UNSIGNED`, ignored on Vercel).
- **Diagnostics**: `/api/_diag/kv` gated behind `CRON_SECRET`.
- **Debug**: Werkzeug debugger gated behind `FLASK_DEBUG` (never default on).
- **XSS**: frontend escapes all user/remote-controlled strings via
  `escHtml()` before `innerHTML`.
- **Data deletion**: `POST /api/account/delete` (GDPR/CCPA erasure) purges
  all per-user data and cancels any active Stripe subscription.

### Known low-severity notes
- The `?code=` share-link can appear in access logs / browser history; the
  client strips it after redeeming. Rotate codes periodically.
- Team access is per-browser and codes are shared secrets (not per-person).

## Environment variables (production)
| Var | Required? | Purpose |
|-----|-----------|---------|
| `SECRET_KEY` | **Yes** | Session signing (app refuses to boot without it on Vercel) |
| `ANTHROPIC_API_KEY` | Optional | Enables the AI Lynch verdict (falls back to DCF when absent) |
| `STRIPE_SECRET_KEY` | For payments | Live `sk_live_…` key |
| `STRIPE_PRICE_ID` | For payments | The live $2/mo recurring price |
| `STRIPE_WEBHOOK_SECRET` | **Yes for payments** | Webhook signature; missing = subscriptions never activate |
| `CRON_SECRET` | For cron/diag | Auth for `/api/cron/*` and `/api/_diag/kv` |
| `ALLOWED_ORIGINS` | Optional | Override CORS allow-list |
| `VALUS_UNLIMITED_CODES` | Optional | Comma-separated team access codes (unlimited access) |
| KV/Redis URL | Recommended | Durable portfolios/subscriptions across cold starts |

## Pre-payment go-live checklist
1. **Stripe live mode**: activate account (identity + payout bank), swap to
   `sk_live_…`, create the live Price, set `STRIPE_PRICE_ID`.
2. **Webhook**: live endpoint at `/api/stripe/webhook` subscribed to
   `checkout.session.completed`, `customer.subscription.updated`,
   `customer.subscription.deleted`; copy the live signing secret to
   `STRIPE_WEBHOOK_SECRET`. **Redeploy** after any env change.
3. **Support inbox**: make `contact@valusfinancial.com` a monitored mailbox
   (required by ToS / Privacy / refund + GDPR requests).
4. **Tax**: enable Stripe Tax on the Checkout session (SaaS is taxable in
   several US states; EU sales trigger VAT).
5. **Data licensing**: move off yfinance/Yahoo (ToS prohibits commercial
   redistribution) to SEC EDGAR (fundamentals, free/public) + a quote
   provider whose plan grants **display/redistribution** rights.
6. **End-to-end test**: real purchase (or Stripe test clock) → webhook →
   VALUS+ granted in KV → cancel → access ends at period end.
7. Set `VALUS_PLUS_EMAILS` empty in production.

## Reporting
Security issues: contact@valusfinancial.com.
