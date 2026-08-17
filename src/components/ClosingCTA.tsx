import { Search } from 'lucide-react';

export default function ClosingCTA() {
  const scrollToSearch = () => {
    const input = document.getElementById('location-search');
    if (!input) return;
    input.scrollIntoView({ behavior: 'smooth', block: 'center' });
    (input as HTMLInputElement).focus({ preventScroll: true });
  };

  return (
    <section className="bg-navy-800 text-white pt-16 pb-8">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="max-w-2xl">
          {/* text-white set directly here, not inherited from the section --
              index.css's global `h1,h2,h3 { text-navy-700 }` base rule is an
              explicit color on the element itself, which wins over the
              parent's text-white (the same gotcha documented in
              DESIGN-SYSTEM-COLOR-PALETTE.md for <a> tags). Without this the
              heading rendered navy-700 on navy-800 -- nearly invisible. */}
          <h2 className="text-2xl md:text-3xl font-heading font-bold text-white mb-3">
            Ready to Find Care?
          </h2>
          <p className="text-primary-200 leading-relaxed mb-6">
            Search accredited home care, home health, and hospice providers near you.
          </p>
          <button
            onClick={scrollToSearch}
            className="btn-secondary bg-white text-neutral-900 hover:bg-neutral-100 inline-flex items-center gap-2 font-bold shadow-md"
          >
            <Search className="w-4 h-4" aria-hidden="true" />
            Search Now
          </button>

          <p className="text-sm text-primary-200 mt-8 pt-6 border-t border-navy-600">
            Can't find what you're looking for?{' '}
            {/* Color set directly on the <a>, not inherited from the
                paragraph -- index.css's global `a { text-primary-600 }`
                rule overrides any parent color otherwise (see the
                documented gotcha in DESIGN-SYSTEM-COLOR-PALETTE.md,
                which is exactly the bug this avoids). */}
            <a
              href="mailto:cindy.rogers@accreditedhomehealthcare.directory"
              className="text-white underline hover:no-underline font-medium"
            >
              Contact us
            </a>{' '}
            and we'll help you find the right care.
          </p>
        </div>
      </div>
    </section>
  );
}
