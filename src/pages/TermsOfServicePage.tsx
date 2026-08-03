import Header from '../components/Header';
import Footer from '../components/Footer';

export default function TermsOfServicePage() {
  return (
    <div className="min-h-screen flex flex-col">
      <Header />

      <main className="flex-1">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <div className="max-w-3xl">
            <h1 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-2">
              Terms of Service
            </h1>
            <p className="text-sm text-neutral-500 mb-8">Last updated August 2, 2026</p>

            <div className="space-y-8 text-neutral-700 leading-relaxed">
              <p>
                These terms cover how you can use the Accredited Home Healthcare Directory (AHHD). By using the site, you agree to them.
              </p>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  What AHHD is
                </h2>
                <p>
                  AHHD is an informational directory. We surface accreditation status for home care, home health, and hospice providers, based on data from recognized accrediting bodies. We don't endorse specific providers, and using the site isn't a substitute for verifying details directly with a provider before choosing them.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Acceptable use
                </h2>
                <p className="mb-3">
                  You're welcome to browse and search AHHD to find care for yourself or someone you're helping. That's what it's here for.
                </p>
                <p>
                  What's not okay: using automated tools (scrapers, bots, crawlers, or similar) to copy, extract, or republish the site's content or data in bulk; reselling or monetizing our data or the way we've organized and presented it; or using it to build a competing directory or service. If you'd like to use AHHD's data for something beyond normal browsing, contact us first.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Accuracy
                </h2>
                <p>
                  We pull accreditation data from the accrediting bodies themselves and keep it updated regularly, but details can change faster than we can reflect them. Always confirm accreditation status and other details directly with a provider before relying on them.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Changes to these terms
                </h2>
                <p>
                  We may update this page as the site changes. Check back here if you want the current version.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Questions
                </h2>
                <p>
                  Email{' '}
                  <a href="mailto:cindy.rogers@accreditedhomehealthcare.directory" className="text-primary-600 hover:text-primary-700 font-medium">
                    cindy.rogers@accreditedhomehealthcare.directory
                  </a>{' '}
                  with any questions about these terms.
                </p>
              </section>
            </div>
          </div>
        </div>
      </main>

      <Footer />
    </div>
  );
}
