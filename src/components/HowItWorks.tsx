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
            Find accredited home care, home health, and hospice providers in your area
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 md:gap-12">
          <div>
            <div className="inline-flex items-center justify-center w-16 h-16 bg-primary-100 rounded-full mb-6">
              <Search className="w-8 h-8 text-primary-600" />
            </div>
            <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
              Search by Location
            </h3>
            <p className="text-neutral-700 leading-relaxed">
              Enter your city, ZIP code, or state to find accredited providers near you. Filter by the type of care you need.
            </p>
          </div>

          <div>
            <div className="inline-flex items-center justify-center w-16 h-16 bg-success-100 rounded-full mb-6">
              <Shield className="w-8 h-8 text-success-600" />
            </div>
            <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
              Check Accreditation Status
            </h3>
            <p className="text-neutral-700 leading-relaxed">
              All providers listed have been accredited by ACHC (Accreditation Commission for Health Care), an independent organization that reviews home care, home health, and hospice providers.
            </p>
          </div>

          <div>
            <div className="inline-flex items-center justify-center w-16 h-16 bg-navy-100 rounded-full mb-6">
              <CheckCircle className="w-8 h-8 text-navy-600" />
            </div>
            <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
              Contact Providers Directly
            </h3>
            <p className="text-neutral-700 leading-relaxed">
              Review provider details and contact them directly. We surface accreditation status, but we don't endorse specific providers.
            </p>
          </div>
        </div>

        <div className="mt-16 bg-neutral-50 rounded-2xl p-8 md:p-12">
          <div className="max-w-3xl">
            <h3 className="text-2xl font-heading font-semibold text-navy-800 mb-4">
              Important Notice
            </h3>
            <p className="text-neutral-700 leading-relaxed text-lg">
              The Accredited Home Healthcare Directory (AHHD) is an informational resource. Listings are based on accreditation data from ACHC (Accreditation Commission for Health Care). We don't endorse specific providers, so always verify details and conduct your own research when choosing a care provider.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
