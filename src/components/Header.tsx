import { Menu, X } from 'lucide-react';
import { useState } from 'react';

export default function Header() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const currentPath = window.location.pathname;

  return (
    <header className="bg-white border-b border-neutral-200 sticky top-0 z-50 shadow-sm">
      <nav className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-20">
          <div className="flex items-center">
            <a href="/" className="flex items-center">
              <img
                src="/ahhd-logo.png"
                alt="Accredited Home Healthcare Directory"
                className="h-12 w-auto"
              />
            </a>
          </div>

          <div className="hidden md:flex items-center space-x-8">
            <a
              href="/"
              className={`font-medium ${
                currentPath === '/'
                  ? 'text-primary-600 border-b-2 border-primary-600 pb-1'
                  : 'text-navy-700 hover:text-primary-600'
              }`}
            >
              Find Care
            </a>
            {/* "About" link removed -- /about has no route yet */}
          </div>

          <button
            className="md:hidden p-2"
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            aria-label="Toggle menu"
            aria-expanded={mobileMenuOpen}
          >
            {mobileMenuOpen ? (
              <X className="w-6 h-6 text-navy-700" />
            ) : (
              <Menu className="w-6 h-6 text-navy-700" />
            )}
          </button>
        </div>

        {mobileMenuOpen && (
          <div className="md:hidden py-4 border-t border-neutral-200">
            <div className="flex flex-col space-y-4">
              <a
                href="/"
                className={`font-medium py-2 ${
                  currentPath === '/'
                    ? 'text-primary-600'
                    : 'text-navy-700 hover:text-primary-600'
                }`}
              >
                Find Care
              </a>
              {/* "About" link removed -- /about has no route yet */}
            </div>
          </div>
        )}
      </nav>
    </header>
  );
}
