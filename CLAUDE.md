# Google Analytics (GA4)

The site's GA4 tag (measurement ID `G-6T34RVV1SL`) lives in `index.html`'s `<head>`, near the top, right after the `<meta charset>` tag:

```html
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-6T34RVV1SL"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag() { dataLayer.push(arguments); }
  gtag('js', new Date());
  gtag('config', 'G-6T34RVV1SL');
</script>
```

This is a single-page app (one Vite entry point, `index.html`, with client-side routing) -- every route the app renders already loads through that one `<head>`, so no per-route wiring is needed for GA4 to see them.

**When this stops being automatic:** if a new page is added as its own standalone HTML entry point (a separate static page outside the SPA's router, a landing page built independently, a new Vite multi-page entry), that new page needs this same tag pasted into its own `<head>` -- it will not inherit `index.html`'s tag. Check `vite.config.ts` for `build.rollupOptions.input` to see whether multiple entry points exist before assuming this is still covered.
