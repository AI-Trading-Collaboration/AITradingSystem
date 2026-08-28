# TRADING-2542G S13 vendor evidence inquiry packet

Status: `PREPARED_NOT_SENT`

Authorization state: `EXACT_PREAUTHORIZED`

Scope: non-executable `DATA_RESEARCH`; capability, historical-version lineage,
internal-use license and exact-price inquiry only. No purchase, provider API query,
real market payload, DQ, veto series, backtest, order, fill, position, production or
broker action is authorized.

## Shared sender placeholders

- Sender first name: `OWNER_INPUT_REQUIRED`
- Sender last name: `OWNER_INPUT_REQUIRED`
- Sender email: `OWNER_INPUT_REQUIRED`
- Sender phone: `OWNER_INPUT_REQUIRED_FOR_CBOE`
- Company or organization: `OWNER_INPUT_REQUIRED_FOR_CBOE`

These values must not be inferred from Git configuration, browser autofill, local
accounts or other machine state.

## FMP inquiry

- Official contact page: <https://site.financialmodelingprep.com/contact>
- Official email shown on that page: `info@financialmodelingprep.com`
- Form fields observed: sender email, inquiry category and message
- Send state: `SEND_BLOCKED_MISSING_AUTHORIZED_SENDER_IDENTITY_OR_CHANNEL`
- Subject: Historical as-of EOD archive, lineage, license and quote for SPY/QQQ research

Proposed message:

> We are evaluating FMP for an internal, non-display research workflow covering
> SPY and QQQ daily prices from at least 2020-03-10 through 2025-12-02. Before any
> purchase or API use, please confirm whether FMP offers a provider-native,
> versioned/as-of archive that can reproduce the payload as it was available at a
> historical decision cutoff, rather than only the current normalized history.
>
> For that exact scope, please describe: (1) per-row publication or available-at
> timestamps and timezone; (2) raw and dividend/split-adjusted close definitions;
> (3) corporate-action adjustment vintage; (4) correction, reissue and supersession
> history; (5) request parameters, schema/version and snapshot checksum or delivery
> receipt; (6) internal non-display research/backtesting rights; and (7) the exact
> product and price. We are not requesting sample or production payloads at this
> stage and are not authorizing a purchase.

## Cboe DataShop inquiry

- Official contact surface: <https://datashop.cboe.com/data-products#contact-us>
- Category: `Sales`
- Required fields observed: first name, last name, phone, email, company, subject,
  message and privacy acknowledgement
- CAPTCHA observed: yes; user interaction is required before any submission
- Send state:
  `SEND_BLOCKED_MISSING_AUTHORIZED_SENDER_IDENTITY_OR_CHANNEL_AND_CAPTCHA_CONFIRMATION`
- Subject: Main Channel EOD VIX historical delivery lineage, license and exact quote

Proposed message:

> We are evaluating Cboe Main Channel End-of-Day Summary as the official VIX source
> for an internal, non-display research workflow covering at least the 2020 warm-up
> period through 2025-12-02. Before any purchase or data delivery, please confirm
> whether the product can provide the original daily files for the requested dates
> together with evidence sufficient to reconstruct what was available at each
> historical cutoff.
>
> Please describe: (1) exact VIX level/session and timezone mapping; (2) delivery
> timestamp or receipt for each daily file; (3) file digest; (4) correction, reissue
> and supersession ledger, including whether prior file versions remain obtainable;
> (5) historical publication/availability vintage; (6) internal non-display
> research/backtesting rights; and (7) the exact product configuration and quote.
> We are not requesting sample or production payloads at this stage and are not
> authorizing a purchase.

## Submission boundary

Submitting either inquiry is representational communication. Even after sender
identity is supplied, the final browser submit action requires an immediate,
destination-specific confirmation. A prepared packet or opened contact page does
not count as a contact attempt or sent message.
