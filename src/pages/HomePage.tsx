import Header from '../components/Header';
import Footer from '../components/Footer';
import SearchHero from '../components/SearchHero';
import HowItWorks from '../components/HowItWorks';
import ClosingCTA from '../components/ClosingCTA';
import { Award, Heart } from 'lucide-react';

export default function HomePage() {
  return (
    <div className="min-h-screen flex flex-col">
      <Header />

      <main className="flex-1">
        <SearchHero />

        <HowItWorks />

        <section className="py-20 bg-neutral-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            {/* Left-aligned to the shared max-w-7xl edge (see
                UI-LAYOUT-STYLE-GUIDE.md) instead of centered. */}
            <div className="mb-16">
              <h2 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-4">
                Understanding Accreditation
              </h2>
              <p className="text-xl text-neutral-700 max-w-3xl leading-relaxed">
                What accreditation means, and why it's part of every listing on this site
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              <div className="card p-8">
                <div className="flex items-center gap-3 mb-3">
                  {/* Circle-badge icon treatment, matching TrustStats --
                      the icon sits inline beside the heading here (not
                      stacked above it like HowItWorks' 3-step cards), so
                      TrustStats' inline badge is the structural match,
                      just scaled for a card heading instead of a stat. */}
                  <div className="w-11 h-11 md:w-12 md:h-12 bg-primary-100 rounded-full flex items-center justify-center flex-shrink-0">
                    <Award className="w-5 h-5 md:w-6 md:h-6 text-primary-600" />
                  </div>
                  <h3 className="text-xl font-heading font-semibold text-navy-800">
                    What is Accreditation?
                  </h3>
                </div>
                <p className="text-neutral-700 leading-relaxed">
                  Accreditation is a voluntary process where healthcare organizations are evaluated by independent accreditation bodies to confirm they meet recognized standards for care quality, patient safety, and operations. The providers listed on this site are accredited by ACHC (Accreditation Commission for Health Care), an independent, nonprofit organization that accredits home health, hospice, and other in-home care providers.
                </p>
              </div>

              <div className="card p-8">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-11 h-11 md:w-12 md:h-12 bg-success-100 rounded-full flex items-center justify-center flex-shrink-0">
                    <Heart className="w-5 h-5 md:w-6 md:h-6 text-success-600" />
                  </div>
                  <h3 className="text-xl font-heading font-semibold text-navy-800">
                    Why It Matters
                  </h3>
                </div>
                <p className="text-neutral-700 leading-relaxed">
                  Choosing an accredited provider means choosing an organization that has been reviewed by an independent body and found to meet recognized standards. It provides an additional layer of assurance during important care decisions.
                </p>
              </div>
            </div>


          </div>
        </section>

        <ClosingCTA />
      </main>

      <Footer />
    </div>
  );
}
