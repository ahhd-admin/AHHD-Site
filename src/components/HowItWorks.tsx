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
            <div className="inline-flex items-center justify-center w-16 h-16 bg-success-100 rounded-full mb-6">
              <Shield className="w-8 h-8 text-success-600" />
            </div>
            <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
              We Find Accredited Providers
            </h3>
            <p className="text-neutral-700 leading-relaxed">
              AHHD brings together home care, home health, and hospice providers who hold real accreditation from a recognized accrediting body, so every listing here has already been vetted.
            </p>
          </div>

          <div>
            <div className="inline-flex items-center justify-center w-16 h-16 bg-primary-100 rounded-full mb-6">
              <Search className="w-8 h-8 text-primary-600" />
            </div>
            <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
              You Search by Location
            </h3>
            <p className="text-neutral-700 leading-relaxed">
              Enter your city, ZIP code, or state to find accredited providers near where care is needed. Filter by the type of care you're looking for.
            </p>
          </div>

          <div>
            <div className="inline-flex items-center justify-center w-16 h-16 bg-navy-100 rounded-full mb-6">
              <CheckCircle className="w-8 h-8 text-navy-600" />
            </div>
            <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
              You Connect With Confidence
            </h3>
            <p className="text-neutral-700 leading-relaxed">
              Reach out to a provider directly, already knowing they hold current accreditation. We don't endorse specific providers, so it's still worth confirming details yourself.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
