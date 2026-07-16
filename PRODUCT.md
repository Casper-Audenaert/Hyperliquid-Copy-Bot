# Product

## Register

product

## Users

A solo (or small-group) crypto trader running HL Sim Desk to simulate copy-trading multiple Hyperliquid wallets before ever risking real capital. They add wallets they're considering following, watch simulated equity/PnL/risk stats accrue in real time over days-to-months, and use that evidence to decide which traders are actually worth copying with real money. Sessions range from quick mobile glances (checking overnight PnL) to extended desktop monitoring (comparing multiple wallets side by side, reading the tearsheet). The tool runs 24/7 unattended; the user checks in, not the reverse.

## Product Purpose

A multi-wallet copy-trading simulator and decision-support tool. Every wallet the user adds gets its own fully independent simulated portfolio (own equity curve, positions, trade history) mirroring a real target trader's fills, scaled to a chosen starting balance. The product's job is to answer one question per wallet, continuously and evidence-backed: **should I copy this trader with real money?** Success looks like a user who can glance at a wallet's decision widget and trust the verdict, and who never has that trust broken by a stale, empty, or crashed chart after the app has been running unattended for months.

## Brand Personality

Precise. Unshowy. Data-first. This is a working instrument for a decision with real financial stakes (even though the trading itself is simulated) — it should feel like a serious analyst's tool, not a consumer app or a marketing site. Confidence comes from clarity and density done well, not from decoration. Motion and color are used with intent (a state changed, a value updated) rather than for flourish.

Direction confirmed with the user: a **refined evolution** of the current system, not a wholesale identity change — keep the dark navy (`#0A0E1A`) / violet (`#7C6CFF`) foundation and the Space Grotesk + JetBrains Mono pairing, but raise the craft: tighter spacing and rhythm, sharper typographic hierarchy, more deliberate motion and hover/focus states, and less of a "default admin dashboard" feel in the component shapes themselves.

## Anti-references

**Generic SaaS admin dashboard** — identical same-sized stat cards tiled in a grid, gradient hero banners, generic icon-set illustrations, templated empty states. If a component could be swapped into any CRUD admin panel without anyone noticing, it's wrong for this product.

Also avoid (general house rules, not specific to this project): crypto/web3 hype aesthetics (neon gradients, glassmorphism-as-decoration, glowing 3D orbs), and anything that reads as playful/consumer (bubbly rounded UI, bright multi-color palettes) — this handles P&L on (simulated) real money and should read as serious throughout.

## Design Principles

1. **Data density with hierarchy, not data density as noise.** This tool's whole value is showing a lot of numbers at once (KPIs, positions, trade feed, tearsheet) — the redesign's job is sharper visual hierarchy and rhythm so density reads as "organized instrument panel," not "wall of numbers."
2. **Green/red is a promise, not a palette.** Color that means P&L direction must never appear for anything else. Every other piece of state (selection, focus, warning, live/paused) gets its own distinct visual language so the P&L signal never has to compete for attention.
3. **Trust is earned by the chart never lying.** The single most important interaction in this product is glancing at an equity curve and believing it. Layout and motion choices should never make a chart look broken, reset, or stale even for a moment — this is a harder constraint here than in most dashboards, because the app is meant to run unattended for months.
4. **Desktop is the workstation, mobile is the glance.** Desktop layout can afford real depth (multi-panel comparison, dense tearsheet). Mobile is optimized for the 10-second "how's it doing" check, not a cramped clone of the desktop layout.
5. **Motion signals state change, not decoration.** Every animation should answer "what just changed" (a new fill, a KPI updating, a tab switching) — never present purely for polish's own sake, per the brand's "unshowy" personality.

## Accessibility & Inclusion

WCAG AA contrast (4.5:1 body text, 3:1 large text/UI) is the bar — confirmed with the user as sufficient for this personal-use tool; no additional accommodation requirements beyond that.
