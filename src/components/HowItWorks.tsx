import { Search, Shield, CheckCircle } from 'lucide-react';

export default function HowItWorks() {
  return (
    <section className="py-20 bg-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Left-aligned to the shared max-w-7xl edge (see
            UI-LAYOUT-STYLE-GUIDE.md) instead of centered -- consistent
            with the rest of the page. */}
        <div className="mb-16">
          <h2 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-4">
            How AHHD Works
          </h2>
          <p className="text-xl text-neutral-700 max-w-3xl leading-relaxed">
            From verified accreditation to your first conversation with a provider.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 md:gap-12">
          <div>
            {/* Icon inline beside the heading, not stacked above it --
                matches Understanding Accreditation's card treatment (see
                HomePage.tsx) so the same "icon badge + heading" pattern
                reads consistently everywhere on the site, not just here
                when stacked at narrower widths. */}
            <div className="flex items-center gap-3 mb-3">
              <div className="w-11 h-11 md:w-12 md:h-12 bg-success-100 rounded-full flex items-center justify-center flex-shrink-0">
                <Shield className="w-5 h-5 md:w-6 md:h-6 text-success-600" aria-hidden="true" />
              </div>
              <h3 className="text-xl font-heading font-semibold text-navy-800">
                We Find Accredited Providers
              </h3>
            </div>
            <p className="text-neutral-700 leading-relaxed">
              AHHD brings together home care, home health, and hospice providers who hold real accreditation from a recognized accrediting body, so every listing here has already been vetted.
            </p>
          </div>

          <div>
            <div className="flex items-center gap-3 mb-3">
              <div className="w-11 h-11 md:w-12 md:h-12 bg-primary-100 rounded-full flex items-center justify-center flex-shrink-0">
                <Search className="w-5 h-5 md:w-6 md:h-6 text-primary-700" aria-hidden="true" />
              </div>
              <h3 className="text-xl font-heading font-semibold text-navy-800">
                You Search by Location
              </h3>
            </div>
            <p className="text-neutral-700 leading-relaxed">
              Enter your city, ZIP code, or state to find accredited providers near where care is needed. Filter by the type of care you're looking for.
            </p>
          </div>

          <div>
            <div className="flex items-center gap-3 mb-3">
              <div className="w-11 h-11 md:w-12 md:h-12 bg-navy-100 rounded-full flex items-center justify-center flex-shrink-0">
                <CheckCircle className="w-5 h-5 md:w-6 md:h-6 text-navy-600" aria-hidden="true" />
              </div>
              <h3 className="text-xl font-heading font-semibold text-navy-800">
                You Connect With Confidence
              </h3>
            </div>
            <p className="text-neutral-700 leading-relaxed">
              Reach out to a provider directly, already knowing they hold current accreditation. We don't endorse specific providers, so it's still worth confirming details yourself.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
