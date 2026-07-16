---
name: HL Sim Desk
description: A copy-trading simulation instrument panel for evaluating Hyperliquid traders before risking real capital
colors:
  base-void:        "#0A0E1A"
  surface-1:        "#111726"
  surface-2:        "#161E30"
  surface-3:        "#1C2540"
  ink-primary:      "#E8EDF7"
  ink-secondary:    "#8A98B5"
  ink-muted:        "#4A5A7A"
  hairline:         "rgba(255,255,255,0.07)"
  signal-violet:    "#7C6CFF"
  signal-violet-deep: "#5548CC"
  profit-green:     "#16C784"
  loss-red:         "#F0506A"
  caution-amber:    "#F5A524"
  base-void-light:     "#F0F4FA"
  surface-1-light:     "#FFFFFF"
  surface-2-light:     "#F4F7FC"
  surface-3-light:     "#E8EDF5"
  ink-primary-light:   "#0F1626"
  ink-secondary-light: "#5A6782"
  ink-muted-light:     "#9AAABF"
  signal-violet-light: "#5D4FDB"
typography:
  display:
    fontFamily: "'Space Grotesk', -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "26px"
    fontWeight: 700
    lineHeight: 1
    letterSpacing: "-0.7px"
  display-secondary:
    fontFamily: "'Space Grotesk', -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "15px"
    fontWeight: 700
    lineHeight: 1
    letterSpacing: "-0.3px"
  title:
    fontFamily: "'Space Grotesk', -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "15px"
    fontWeight: 700
    lineHeight: 1.2
    letterSpacing: "-0.4px"
  label:
    fontFamily: "'Space Grotesk', -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "9px"
    fontWeight: 700
    lineHeight: 1.3
    letterSpacing: "1px"
  body:
    fontFamily: "'Space Grotesk', -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "13px"
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: "normal"
  numeric:
    fontFamily: "'JetBrains Mono', 'Cascadia Code', monospace"
    fontSize: "13px"
    fontWeight: 500
    lineHeight: 1.3
    letterSpacing: "normal"
rounded:
  sm: "6px"
  md: "8px"
  lg: "12px"
  full: "999px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "24px"
  2xl: "32px"
  3xl: "48px"
components:
  button-primary:
    backgroundColor: "{colors.signal-violet}"
    textColor: "#0A0E1A"
    typography: "{typography.body}"
    rounded: "{rounded.sm}"
    padding: "6px 14px"
  button-primary-hover:
    backgroundColor: "{colors.signal-violet-deep}"
  button-ghost:
    backgroundColor: "{colors.surface-2}"
    textColor: "{colors.ink-secondary}"
    rounded: "{rounded.sm}"
    padding: "6px 14px"
  button-ghost-hover:
    textColor: "{colors.ink-primary}"
  card-wallet:
    backgroundColor: "{colors.surface-2}"
    rounded: "{rounded.md}"
    padding: "{spacing.md}"
  card-wallet-selected:
    backgroundColor: "rgba(124,108,255,0.10)"
---

# Design System: HL Sim Desk

## 1. Overview

**Creative North Star: "The Instrument Panel"**

HL Sim Desk is not a SaaS product being sold to someone — it's an instrument a trader built for themselves, checked daily, trusted with a real financial decision. Every element on screen earns its place by carrying information; nothing exists to look impressive. The system rejects the generic-admin-dashboard reflex explicitly: no identical stat-card grids, no gradient hero banners, no decorative icon sets. Where a typical dashboard reaches for a big rounded card with a soft shadow and an icon, this system reaches for a tight number, a hairline border, and a color that means something specific.

The palette stays deliberately narrow. A near-black void hosts three ascending surface tones (used for depth, not decoration), two text-weight tones plus a barely-there muted tone, one signal color (violet) for brand/selection state, and two colors — green and red — that mean exactly one thing each: profit and loss. Nothing else in the interface is allowed to borrow green or red. Typography pairs a geometric grotesque for labels and structure against a monospace for every number, so numeric columns align and the eye can scan a column of P&L without re-parsing font metrics row to row.

This redesign is a refined evolution, not a reinvention — the palette, type pairing, and information architecture already earned their keep. What changes is craft: tighter spacing rhythm (a real 4px-based scale replacing ad-hoc padding values), sharper hierarchy between primary and supporting numbers, more deliberate motion tied to actual state changes, and a touch of ambient elevation on interactive surfaces so the tool feels tactile without ever feeling soft or consumer-grade.

**Key Characteristics:**
- Dense by design — an instrument panel, not a landing page
- One signal color (violet), two meaning-locked colors (green/red), everything else is tone
- Monospace for every number, geometric sans for every label
- Flat surfaces at rest; shadow appears only as a response to hover/focus/float
- Motion answers "what just changed," never decoration for its own sake

## 2. Colors

A near-monochrome dark foundation (four tonal steps from void to raised surface) with exactly one signal color and two meaning-locked semantic colors. Chroma is rationed on purpose — spend it only where it communicates.

### Primary
- **Signal Violet** (`#7C6CFF`): The single brand/interactive accent. Selection state, primary buttons, active tab indicators, focus rings, the logo mark. Used sparingly — if more than roughly 10% of a screen is violet, something has drifted from accent into decoration.
- **Signal Violet Deep** (`#5548CC`): Hover/pressed state for violet elements, and the dark end of the logo mark's gradient.

### Neutral
- **Base Void** (`#0A0E1A`): The page background. Nearly black, cool-leaning.
- **Surface 1** (`#111726`): Header, sidebar, panel backgrounds — the first tonal step up from void.
- **Surface 2** (`#161E30`): Cards, buttons, inputs — the interactive-surface layer.
- **Surface 3** (`#1C2540`): The highest tonal step — icon buttons, chips, anything that sits visually "on top of" a card.
- **Ink Primary** (`#E8EDF7`): Primary text, headline numbers.
- **Ink Secondary** (`#8A98B5`): Supporting text, labels, secondary numbers.
- **Ink Muted** (`#4A5A7A`): Tertiary text, timestamps, disabled state, rank numbers.
- **Hairline** (`rgba(255,255,255,0.07)`): Every border in the system. Never a solid gray — always a low-alpha white over the surface tone so it reads correctly at any elevation.

### Named Rules
**The Meaning Lock Rule.** Green and red exist for exactly one purpose each — profit and loss. They never appear as a decorative accent, a brand color, a chart palette filler, or a "success/error" UI color for anything unrelated to P&L. If a toast or status needs green/red-adjacent meaning ("connected"/"failed"), it borrows the signal violet or amber vocabulary instead, never the P&L pair.

**The One Signal Rule.** Violet is the only color allowed to mean "this is interactive/selected/branded." A screen that reaches for a second accent color to differentiate UI state has a hierarchy problem, not a color problem — solve it with weight, size, or position first.

A parallel light theme exists (`Base Void Light #F0F4FA` / `Surface 1 Light #FFFFFF` / `Signal Violet Light #5D4FDB`, etc.) using the identical role structure at inverted lightness — the same rules apply in either theme.

## 3. Typography

**Display/Label Font:** Space Grotesk (with `-apple-system, BlinkMacSystemFont, sans-serif` fallback)
**Numeric Font:** JetBrains Mono (with `'Cascadia Code', monospace` fallback)

**Character:** A geometric grotesque carries every label, heading, and piece of prose; a monospace carries every number, without exception. The pairing exists for one functional reason — tabular alignment — not for aesthetic contrast. A user should never have to re-read a column of numbers because the digits didn't line up.

### Hierarchy
- **Display** (700, 26px, letter-spacing -0.7px): The hero-tier KPI values only (Equity, Realized PnL, Unrealized PnL, Win Rate) — the four numbers worth a glance from across the room. Genuinely large, not just "big for a dashboard."
- **Display Secondary** (700, 15px, letter-spacing -0.3px): Everything else in the KPI strip (Free Cash, Trades, Fees, Sharpe, Max Drawdown) plus wallet-card equity figures in the sidebar — supporting numbers, deliberately quieter than the hero tier so the type scale itself carries the hierarchy (see the KPI Card component below).
- **Title** (700, 15px, letter-spacing -0.4px): Panel titles, wallet names, section headers.
- **Body** (400, 13px): Prose, descriptions, form labels. The base UI size.
- **Numeric** (500, 13px, JetBrains Mono, tabular-nums): Every number in a table, feed row, or secondary stat — always this weight/family even when embedded inline in a body-font sentence.
- **Label** (700, 9px, letter-spacing 1px, uppercase): Section eyebrows, KPI card labels, table headers. Used structurally, not decoratively — see Don'ts.

### Named Rules
**The Tabular Number Rule.** Any digit sequence a user might scan vertically (a table column, a KPI grid, a trade feed) is always JetBrains Mono with `font-variant-numeric: tabular-nums`. No exceptions, including single inline numbers next to ones that will eventually sit in a column.

## 4. Elevation

Confirmed direction for this redesign: move from "flat everywhere except true overlays" to **subtle ambient layering** — surfaces gain a soft, low-opacity shadow on hover/active/selected states so interactive elements feel tactile, while resting surfaces stay flat. This is a deliberate step up from the prior all-flat system, not a wholesale shift to glassy/heavy elevation — restraint stays the rule, just applied one notch further than before.

### Shadow Vocabulary
- **Ambient hover** (`box-shadow: 0 4px 16px rgba(0,0,0,0.24)`): Wallet cards, compare cards, buttons on hover — a soft lift signaling "this responded to you."
- **Selected glow** (`box-shadow: 0 0 0 1px var(--brand-b), 0 4px 20px rgba(124,108,255,0.15)`): The active/selected wallet card or tab — combines a violet ring with a faint violet-tinted glow, distinct from the neutral hover shadow so "selected" never reads as merely "hovered."
- **Float** (`box-shadow: 0 8px 32px rgba(0,0,0,0.4)`): Dropdowns, tooltips, popovers — anything positioned above the base layer without a full backdrop.
- **Modal** (`box-shadow: 0 40px 80px rgba(0,0,0,0.6), 0 0 0 1px rgba(255,255,255,0.04)`): Full modal dialogs, paired with a `backdrop-filter: blur(8px)` scrim.
- **Drawer** (`box-shadow: 4px 0 24px rgba(0,0,0,0.5)`): The mobile sidebar drawer sliding over content.

### Named Rules
**The Response-Only Rule.** No shadow appears on a surface at rest. Every shadow in the system is triggered by state — hover, focus, selection, or genuine z-axis floating (modal, drawer, dropdown). A card that has a shadow before you touch it is decorating, not communicating.

## 5. Components

### Buttons
- **Shape:** 6px radius (`--r`), tight rectangular proportions — never pill-shaped except badges/pills, which are a distinct component.
- **Primary:** Signal Violet background, near-black text, 6px 14px padding. Reserved for the single most important action in a given context (Add Wallet, Save).
- **Ghost (default):** Surface 2 background, hairline border, Ink Secondary text. The default button treatment — most actions are ghost, not primary, keeping violet rare and meaningful.
- **Hover / Focus:** 150ms ease-out color transition plus the Ambient Hover shadow; focus-visible adds a 2px Signal Violet outline offset 2px from the element, never relying on color change alone.
- **Icon buttons:** Square, Surface 3 background, used for compact repeated actions (reset/remove on a wallet card) — deliberately smaller and quieter than primary/ghost buttons since they're secondary, high-frequency actions.

### Cards / Containers
- **Corner Style:** 8px radius (`--r`) for cards, 12px (`--r2`) for panels — panels are always slightly softer-cornered than the cards inside them, a small but consistent nesting cue.
- **Background:** Surface 2 at rest; a violet-tinted background (`rgba(124,108,255,0.10)`) plus the Selected Glow shadow when active/selected — color and elevation change together, never one without the other.
- **Border:** 1px Hairline always; brightens to `rgba(255,255,255,0.15)` on hover (dark theme) as a secondary hover cue alongside the shadow.
- **Internal Padding:** `spacing.md` (12px) as the default card padding; `spacing.lg` (16px) for panel chrome (headers, wrappers).

### Inputs / Fields
- **Style:** Surface 2 background, Hairline border, 6px radius, Body typography.
- **Focus:** Border shifts to Signal Violet, no glow/shadow (inputs stay flat — the Response-Only elevation rule applies to surfaces users click into, not type into).
- **Error:** Border shifts to Loss Red with a matching red-tinted background tint, never relying on a small icon alone to signal invalid state.

### Navigation
- **Sidebar (desktop):** Fixed 244px column, Surface 1 background, wallet cards stacked with `spacing.sm` gaps. Selected wallet uses the card-selected treatment (violet tint + glow), not a separate "active nav item" style — the wallet card IS the nav item.
- **Bottom tab bar (mobile, <768px):** Fixed bottom bar, Surface 1 background, compact per-wallet buttons (initials + live return %) replacing the sidebar for one-tap switching without opening the drawer.
- **Drawer (mobile):** The full sidebar becomes an overlay drawer under 768px, sliding in with the Drawer shadow over a dimmed scrim.

### KPI Card (signature component)
The KPI strip is the system's most-repeated pattern and its clearest expression of "instrument panel over dashboard cliché": a dense row of Label/Display pairs in a hairline-divided strip (not individual boxed cards with shadows and icons), auto-fitting to available width so the count of KPIs can grow without needing hand-tuned breakpoints. It's split into two stacked tiers, sharing one visual container: a **hero tier** (Equity, Realized PnL, Unrealized PnL, Win Rate) at Display scale with more generous padding, and a **secondary tier** (Free Cash, Trades, Fees, Sharpe, Max Drawdown) at Display Secondary scale on a marginally darker strip background, denser padding. The hierarchy is entirely type-scale-and-density-driven — no per-tile shadows, borders, or icons distinguish "important" from "supporting." A KPI's value flashes a brief profit-green or loss-red background tint (600ms fade) when it changes — the system's primary "something happened" motion cue, and it works identically in both tiers.

### Copy Decision Gauge (signature component)
A hand-drawn SVG speedometer (three static color-zone arcs — red 0-34, amber 35-64, green 65-100 — plus a needle rotated via CSS transform) is the system's one deliberately illustrative element, earning its place because it's the product's single most important glanceable verdict (COPY/MONITOR/SKIP). No other component in the system uses a custom illustrative shape; this is the exception that proves the "nothing decorative" rule by being the one thing worth breaking it for.

## 6. Do's and Don'ts

### Do:
- **Do** use JetBrains Mono with `tabular-nums` for every number that could ever sit in a column, even a lone KPI value.
- **Do** ration Signal Violet to interactive/selected/branded meaning only — if you need a second "this matters" color, that's a hierarchy problem to solve with size/weight/position, not a new color.
- **Do** keep shadows response-only: hover, focus, selection, or genuine floating (modal/drawer/dropdown). Nothing gets a shadow at rest.
- **Do** build spacing from the 4px-based scale (`xs 4 / sm 8 / md 12 / lg 16 / xl 24 / 2xl 32 / 3xl 48`) rather than ad-hoc pixel values, so density reads as rhythm rather than clutter.
- **Do** tie every animation to a real state change (a value updated, a tab switched, a new fill arrived) — 150-220ms ease-out, no bounce/spring.

### Don't:
- **Don't** build a generic SaaS admin dashboard — no identical same-sized stat-card grids, no gradient hero banners, no generic icon-set illustrations. (Direct anti-reference from PRODUCT.md.)
- **Don't** let green or red mean anything except profit and loss. Not "success," not "online," not a chart series color — those borrow violet/amber instead.
- **Don't** use `border-left`/`border-right` as a colored accent stripe on cards or list rows — use full borders, background tints, or nothing. (The position cards did exactly this — `border-left:3px solid var(--green/--red)` — before this redesign; fixed with a `color-mix()` full-card background tint instead, letting the existing LONG/SHORT badge carry the actual signal.)
- **Don't** use `background-clip: text` gradient text for emphasis — a single solid ink color plus weight/size does the job.
- **Don't** reach for glassmorphism as decoration — the one `backdrop-filter: blur()` in the system is functional (the modal scrim), not aesthetic.
- **Don't** add a shadow to a resting card, panel, or KPI — see the Response-Only Rule in Elevation.
- **Don't** introduce a second illustrative/custom-drawn component beyond the Copy Decision Gauge — if a second thing wants an icon or illustration, reconsider whether it needs one.
