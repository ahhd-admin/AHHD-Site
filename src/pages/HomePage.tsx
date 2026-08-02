import Header from '../components/Header';
import Footer from '../components/Footer';
import SearchHero from '../components/SearchHero';
import HowItWorks from '../components/HowItWorks';
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
                Accreditation is a voluntary review process where an independent organization checks that a healthcare provider meets nationally recognized standards for care quality and safety
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              <div className="card p-8">
                <div className="flex items-start gap-4">
                  <div className="flex-shrink-0">
                    <Award className="w-10 h-10 text-primary-600" />
                  </div>
                  <div>
                    <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                      What is Accreditation?
                    </h3>
                    <p className="text-neutral-700 leading-relaxed">
                      Accreditation is a voluntary process where healthcare organizations are evaluated by independent accreditation bodies to confirm they meet recognized standards for care quality, patient safety, and operations. The providers listed on this site are accredited by ACHC (Accreditation Commission for Health Care), an independent, nonprofit organization that accredits home health, hospice, and other in-home care providers.
                    </p>
                  </div>
                </div>
              </div>

              <div className="card p-8">
                <div className="flex items-start gap-4">
                  <div className="flex-shrink-0">
                    <Heart className="w-10 h-10 text-success-600" />
                  </div>
                  <div>
                    <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                      Why It Matters
                    </h3>
                    <p className="text-neutral-700 leading-relaxed">
                      Choosing an accredited provider means choosing an organization that has been reviewed by an independent body and found to meet recognized standards. It provides an additional layer of assurance during important care decisions.
                    </p>
                  </div>
                </div>
              </div>
            </div>


          </div>
        </section>


      </main>

      <Footer />
    </div>
  );
}
