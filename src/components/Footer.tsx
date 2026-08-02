export default function Footer() {
  return (
    <footer className="bg-navy-800 text-white">
      {/* No top margin here -- it used to be mt-24, which assumed a light
          section always precedes the footer and needed breathing room
          before the dark band. That broke once a dark section (ClosingCTA)
          could sit directly above it: two navy blocks with an unwanted gray
          gap between them instead of flowing together. Spacing before the
          footer is now each page's own responsibility (e.g. ClosingCTA's
          py-16, ProviderDetailPage's mt-12 before its last section). */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Full Quick Links / Legal columns still removed -- /about and
            /terms don't exist yet, only /privacy does so far. Re-add the
            fuller footer layout once those are built. */}
        <div className="text-neutral-400">
          <p className="text-xs mb-1.5">
            AHHD is an informational directory and does not endorse specific providers.
          </p>
          <p className="mb-2">&copy; 2026 Accredited Home Healthcare Directory</p>
          {/* Its own small, separate line -- not inline with the copyright
              text -- since it's a distinct kind of link (legal reference,
              not attribution) and doesn't need equal visual weight.
              Color set directly on the <a>, not inherited -- index.css's
              global `a { text-primary-600 }` rule overrides any parent
              color otherwise (see the documented gotcha in
              DESIGN-SYSTEM-COLOR-PALETTE.md). */}
          <a href="/privacy" className="text-xs text-neutral-400 hover:text-white underline">
            Privacy Policy
          </a>
        </div>
      </div>
    </footer>
  );
}
