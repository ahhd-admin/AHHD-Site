# UI Layout Style Guide

_How pages and sections should be structured on the AHHD site. For copy tone/wording, see `SITE-COPY-GUIDELINES.md`. For color/contrast, see `DESIGN-SYSTEM-COLOR-PALETTE.md` (in the `achcscraper` root)._

## Alignment: left-aligned by default, no centering

Every section on the site shares one container:

```html
<div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
```

(`SearchHero.tsx` uses `px-3` instead of `px-4` on mobile only, to claw back a few px for the search panel on narrow screens — the `sm:`/`lg:` breakpoints match everywhere else.)

Within that container, **text and content are left-aligned, not centered**. This replaced an earlier centered layout across the whole home page (headings, icon cards, the footer copyright line). The reason: most sections sit above or beside asymmetric content — a two-column search/results split, a three-card grid, an icon-plus-heading card — and centering text independently of that content produces a page that looks "off" even when each piece is individually well-centered. Left-aligning everything to the same `max-w-7xl` edge gives the page one consistent vertical line to align against, rather than several independently-centered blocks floating above asymmetric content (the "Swiss grid" reference point for this).

**Rule of thumb:** don't add `text-center` or `mx-auto` on a text block or heading unless that specific element is genuinely standalone and symmetric (e.g. a centered modal, a single icon with no adjacent asymmetric content). If you're about to center something that sits above a multi-column layout, left-align it instead.

Body copy that needs a readable line length still gets `max-w-3xl` (or similar) to cap width — that's fine and expected. Just don't center the block itself.

## Container/section pattern

```jsx
<section className="py-20 bg-white">
  <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
    <div className="mb-16">
      <h2 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-4">
        Section Heading
      </h2>
      <p className="text-xl text-neutral-700 max-w-3xl leading-relaxed">
        Supporting copy, left-aligned, width-capped for readability.
      </p>
    </div>
    {/* section content */}
  </div>
</section>
```

Match `py-20` (or the section's existing vertical rhythm) and the heading scale (`text-3xl md:text-4xl` for section headings, `text-xl font-semibold` for card-level subheadings) to what's already on the page rather than inventing a new scale per section.

## Accessibility baseline

Carried over from the accessibility audit pass — apply these on any new interactive or dynamic UI, not just the home page:

- Dynamic result counts / status text that updates without a page reload: `aria-live="polite"`.
- Loading spinners: `role="status"` plus `aria-hidden` on the visual spinner element and `sr-only` text describing what's loading.
- Toggle buttons (map/grid view, expand/collapse, mobile menu): `aria-expanded={state}`.
- Map markers: pass a descriptive `title` (maps to `aria-label` on `AdvancedMarker`).
- Minimum mobile tap target: 44px in the dimension a thumb actually taps, even if the visual/desktop size is smaller — verify by measuring, not just by trusting a `h-full` on a flex child (has produced unexpected computed heights before; prefer an explicit `h-9`/`h-11` etc. and confirm in the browser).

## Copy

Don't lean on em dashes to join clauses — see `SITE-COPY-GUIDELINES.md`. Use a period, a comma, or a shorter sentence instead. This applies to all visible site copy; code comments are exempt.
