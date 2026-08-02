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
        {/* Quick Links / Legal columns removed for now -- /find-care is the
            only route that currently exists; /about, /privacy, /terms don't.
            Re-add once those pages are built. */}
        <div className="text-neutral-400">
          <p>&copy; 2026 Accredited Home Healthcare Directory</p>
        </div>
      </div>
    </footer>
  );
}
