import Header from '../components/Header';
import Footer from '../components/Footer';

export default function PrivacyPolicyPage() {
  return (
    <div className="min-h-screen flex flex-col">
      <Header />

      <main className="flex-1">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <div className="max-w-3xl">
            <h1 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-2">
              Privacy Policy
            </h1>
            <p className="text-sm text-neutral-500 mb-8">Last updated August 2, 2026</p>

            <div className="space-y-8 text-neutral-700 leading-relaxed">
              <p>
                The Accredited Home Healthcare Directory (AHHD) is an informational directory. This page explains what information we collect when you use the site and how we handle it.
              </p>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  What we collect
                </h2>
                <p className="mb-3">
                  When you search for care, we process the location and care-type filters you enter to show you matching providers. If you use the "My Location" button, your browser shares your approximate location with us for that search only. We don't create an account for you, and we don't ask for your name, email, or other personal details to use the search.
                </p>
                <p>
                  Your browser also saves a few things locally on your own device, like your last search and map view, so the site feels responsive if you navigate back. This stays on your device and isn't sent to us.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Cookies and analytics
                </h2>
                <p>
                  We ask before using any analytics cookies. If you accept, we use Google Analytics to see which searches and pages are most useful, so we can improve AHHD. We don't use cookies for ads or to track you across other sites. If you decline, that cookie is never set. Either way, the site works exactly the same. Your choice is remembered on your device so we don't ask again on your next visit.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Third-party services we rely on
                </h2>
                <p>
                  Search results and map display are powered by Google Maps Platform, and provider data is stored with Supabase. Both process the location information you search for as part of providing those services, subject to their own privacy policies.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Contacting a provider or visiting their website
                </h2>
                <p>
                  Provider pages may link out to a provider's own website or to a search engine to help you find one. Once you leave AHHD, that site's own privacy practices apply, not ours.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Emailing us
                </h2>
                <p>
                  If you email us (for example, using the "Contact us" link when you can't find what you're looking for), we'll use that email only to respond to you.
                </p>
              </section>

              <section>
                <h2 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Changes to this policy
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
                  with any questions about this policy.
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
