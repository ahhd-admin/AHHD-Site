import Header from '../components/Header';
import Footer from '../components/Footer';
import { MapPin, Heart, Shield, Star, CheckCircle } from 'lucide-react';

export default function FindCarePage() {
  return (
    <div className="min-h-screen flex flex-col">
      <Header />

      <main className="flex-1">
        <section className="relative bg-gradient-to-br from-navy-800 via-navy-700 to-primary-700 text-white py-20 md:py-32 overflow-hidden">
          <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PGRlZnM+PHBhdHRlcm4gaWQ9ImdyaWQiIHdpZHRoPSI2MCIgaGVpZ2h0PSI2MCIgcGF0dGVyblVuaXRzPSJ1c2VyU3BhY2VPblVzZSI+PHBhdGggZD0iTSAxMCAwIEwgMCAwIDAgMTAiIGZpbGw9Im5vbmUiIHN0cm9rZT0id2hpdGUiIHN0cm9rZS1vcGFjaXR5PSIwLjA1IiBzdHJva2Utd2lkdGg9IjEiLz48L3BhdHRlcm4+PC9kZWZzPjxyZWN0IHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIGZpbGw9InVybCgjZ3JpZCkiLz48L3N2Zz4=')] opacity-30"></div>

          <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
            <h1 className="text-4xl md:text-6xl font-heading font-bold mb-6 text-white">
              Find Accredited Home Care
            </h1>
            <p className="text-xl md:text-2xl text-blue-100 mb-10 max-w-3xl mx-auto leading-relaxed">
              Search a directory of home care, home health, and hospice providers accredited by ACHC (Accreditation Commission for Health Care)
            </p>
            <a
              href="/"
              className="inline-flex items-center gap-2 bg-white text-navy-800 px-8 py-4 rounded-xl text-lg font-semibold hover:bg-blue-50 transition-all shadow-lg hover:shadow-xl transform hover:-translate-y-0.5"
            >
              <MapPin className="w-5 h-5" />
              Search Providers Now
            </a>
          </div>
        </section>

        <section className="py-20 bg-white">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="text-center mb-16">
              <h2 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-4">
                What Accreditation Means
              </h2>
              <p className="text-xl text-neutral-700 max-w-3xl mx-auto leading-relaxed">
                Accreditation is a voluntary review where an independent organization confirms a provider meets recognized standards for care quality and safety
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
              <div className="text-center">
                <div className="inline-flex items-center justify-center w-16 h-16 bg-primary-100 rounded-2xl mb-4">
                  <Shield className="w-8 h-8 text-primary-600" />
                </div>
                <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Independent Review
                </h3>
                <p className="text-neutral-700 leading-relaxed">
                  Providers have been reviewed by an independent accreditation organization and found to meet recognized standards
                </p>
              </div>

              <div className="text-center">
                <div className="inline-flex items-center justify-center w-16 h-16 bg-success-100 rounded-2xl mb-4">
                  <CheckCircle className="w-8 h-8 text-success-600" />
                </div>
                <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Current Status
                </h3>
                <p className="text-neutral-700 leading-relaxed">
                  Accreditation status is checked regularly, but details can change — always confirm directly with the provider
                </p>
              </div>

              <div className="text-center">
                <div className="inline-flex items-center justify-center w-16 h-16 bg-warning-100 rounded-2xl mb-4">
                  <Star className="w-8 h-8 text-warning-600" />
                </div>
                <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Voluntary Process
                </h3>
                <p className="text-neutral-700 leading-relaxed">
                  Providers choose to undergo accreditation to demonstrate they meet recognized care standards
                </p>
              </div>

              <div className="text-center">
                <div className="inline-flex items-center justify-center w-16 h-16 bg-error-100 rounded-2xl mb-4">
                  <Heart className="w-8 h-8 text-error-600" />
                </div>
                <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Information You Can Check
                </h3>
                <p className="text-neutral-700 leading-relaxed">
                  Each listing shows the provider's accreditation status, services, and contact information so you can follow up directly
                </p>
              </div>
            </div>
          </div>
        </section>

        <section className="py-20 bg-neutral-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="text-center mb-16">
              <h2 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-4">
                Types of Care Services
              </h2>
              <p className="text-xl text-neutral-700 max-w-3xl mx-auto leading-relaxed">
                The types of in-home care currently included in this directory
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
              <div className="card p-8">
                <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Home Care
                </h3>
                <p className="text-neutral-700 leading-relaxed">
                  Non-medical assistance with daily activities like bathing, dressing, meal preparation, and companionship for those who need help maintaining independence at home.
                </p>
              </div>

              <div className="card p-8">
                <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Home Health Care
                </h3>
                <p className="text-neutral-700 leading-relaxed">
                  Medical care provided at home by licensed healthcare professionals, including skilled nursing, physical therapy, and other clinical services ordered by a physician.
                </p>
              </div>

              <div className="card p-8">
                <h3 className="text-xl font-heading font-semibold text-navy-800 mb-3">
                  Hospice
                </h3>
                <p className="text-neutral-700 leading-relaxed">
                  Compassionate end-of-life care focused on comfort and quality of life, providing medical care, pain management, and emotional support for patients and families.
                </p>
              </div>
            </div>


          </div>
        </section>

        <section className="py-20 bg-white">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="bg-gradient-to-br from-primary-50 to-blue-50 rounded-3xl p-8 md:p-12 text-center">
              <h2 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-4">
                Ready to Find Care?
              </h2>
              <p className="text-xl text-neutral-700 mb-8 max-w-2xl mx-auto leading-relaxed">
                Search the directory for accredited home care, home health, and hospice providers in your area
              </p>
              <a
                href="/"
                className="inline-flex items-center gap-2 btn-primary text-lg"
              >
                <MapPin className="w-5 h-5" />
                Start Your Search
              </a>
            </div>
          </div>
        </section>
      </main>

      <Footer />
    </div>
  );
}
