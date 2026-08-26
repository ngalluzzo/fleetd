# Fleetd interface system

Fleetd should feel like a dependable operations instrument: quiet during normal
work, unmistakable when state changes, and dense without becoming cryptic. The
visual system is presentation-specific and does not introduce product semantics
into the daemon.

## Visual language

- **Graphite carries hierarchy.** Canvas, shell, inset, and raised surfaces are
  deliberately close in hue and separated by edge contrast and elevation.
- **Ion blue signals intent.** Accent color is reserved for selection, live
  activity, primary action, and keyboard focus. Success, warning, and danger
  keep their conventional operational roles.
- **Type has two jobs.** Native sans is for reading and decisions; monospace is
  for identities, sequence data, statuses, and compact labels. Body copy starts
  at 15px; smaller sizes are metadata rather than primary content.
- **Geometry stays crisp.** Controls use 6px corners, containers 8px, and large
  cards 12px. Full pills are reserved for badges and status capsules.
- **Density follows a 4px grid.** The named spacing scale is the only spacing
  source for shared components. Controls use 32px, 40px, and 46px heights.
- **Elevation is scarce.** Most separation comes from surface and border roles.
  Shadows are reserved for raised cards, overlays, and the active composer.
- **Motion explains state.** 140ms handles direct feedback, 220ms handles state
  transitions, and 360ms is available for rare spatial transitions. Reduced
  motion remains authoritative.

## Foundation contract

`tokens.css` is the single source for color, type, spacing, radius, elevation,
control sizing, and motion. `base.css` owns document and native-control
behavior. `primitives.css` provides layout, surface, brand, badge, icon, and
loading building blocks. Product composition consumes those roles and should
not introduce new raw colors or one-off spacing values.

The accent and status families include named alpha steps for ambient wash,
selection, borders, and focus. Composition selects the semantic role rather
than rebuilding an old palette with literal RGB values. The only literals
outside `tokens.css` are explicit higher-contrast and forced-color platform
adaptations.

All interactive controls must expose a visible `:focus-visible` state. Icons
are decorative unless they have an explicit accessible name, use the shared
one-em optical box, and never become the sole cue for status.
