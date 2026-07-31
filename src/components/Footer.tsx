export default function Footer() {
  return (
    <footer className="bg-navy-800 text-white mt-24">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
          <div>
            <h3 className="text-lg font-heading font-semibold mb-4 text-white">
              AHHD
            </h3>
            <p className="text-neutral-300 text-base leading-relaxed">
              A directory of home care, home health, and hospice providers accredited by ACHC (Accreditation Commission for Health Care).
            </p>
          </div>

          <div>
            <h4 className="text-base font-heading font-semibold mb-4 text-white">
              Quick Links
            </h4>
            <ul className="space-y-2">
              <li>
                <a href="/find-care" className="text-neutral-300 hover:text-primary-400 transition-colors">
                  Find Care
                </a>
              </li>
              <li>
                <a href="/about" className="text-neutral-300 hover:text-primary-400 transition-colors">
                  About
                </a>
              </li>
            </ul>
          </div>

          <div>
            <h4 className="text-base font-heading font-semibold mb-4 text-white">
              Legal
            </h4>
            <ul className="space-y-2">
              <li>
                <a href="/privacy" className="text-neutral-300 hover:text-primary-400 transition-colors">
                  Privacy Policy
                </a>
              </li>
              <li>
                <a href="/terms" className="text-neutral-300 hover:text-primary-400 transition-colors">
                  Terms of Service
                </a>
              </li>
            </ul>
          </div>
        </div>

        <div className="mt-12 pt-8 border-t border-navy-600 text-center text-neutral-400">
          <p>&copy; {new Date().getFullYear()} Accredited Home Healthcare Directory. All rights reserved.</p>
        </div>
      </div>
    </footer>
  );
}
