export default function Footer() {
  return (
    <footer className="bg-navy-800 text-white mt-24">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Quick Links / Legal columns removed for now -- /find-care is the
            only route that currently exists; /about, /privacy, /terms don't.
            Re-add once those pages are built. */}
        <div className="text-center text-neutral-400">
          <p>&copy; 2026 Accredited Home Healthcare Directory</p>
        </div>
      </div>
    </footer>
  );
}
