import { useEffect, useState } from 'react';
import { initAnalyticsIfConsented } from '../lib/analytics';

const CONSENT_KEY = 'ahhd_cookie_consent';

export type CookieConsentChoice = 'accepted' | 'declined';

/** Reads the visitor's stored choice, if any -- used by analytics setup
 * (once wired in) to decide whether it's allowed to load. */
export function getCookieConsent(): CookieConsentChoice | null {
  return localStorage.getItem(CONSENT_KEY) as CookieConsentChoice | null;
}

export default function CookieConsent() {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (!getCookieConsent()) setVisible(true);
  }, []);

  const handleChoice = (choice: CookieConsentChoice) => {
    localStorage.setItem(CONSENT_KEY, choice);
    setVisible(false);
    if (choice === 'accepted') initAnalyticsIfConsented();
  };

  if (!visible) return null;

  return (
    <div
      className="fixed bottom-0 inset-x-0 z-50 bg-navy-800 text-white border-t border-navy-600 shadow-lg"
      role="region"
      aria-label="Cookie notice"
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex flex-col sm:flex-row sm:items-center gap-3 sm:gap-6">
        <p className="text-sm text-primary-100 leading-relaxed flex-1">
          We use Google Analytics cookies to see which searches and pages are most useful, so we can improve AHHD. We don't use them for ads or to track you across other sites. See our{' '}
          {/* Color set directly on the <a>, not inherited -- index.css's
              global `a { text-primary-600 }` rule overrides any parent
              color otherwise (see the documented gotcha in
              DESIGN-SYSTEM-COLOR-PALETTE.md). */}
          <a href="/privacy" className="text-white underline hover:no-underline font-medium">
            Privacy Policy
          </a>{' '}
          for details.
        </p>
        <div className="flex gap-3 flex-shrink-0">
          <button
            onClick={() => handleChoice('declined')}
            className="border-2 border-white text-white hover:bg-white hover:text-navy-800 text-sm px-4 py-2 rounded-lg font-semibold transition-colors"
          >
            Decline
          </button>
          <button
            onClick={() => handleChoice('accepted')}
            className="bg-white text-neutral-900 hover:bg-neutral-100 text-sm px-4 py-2 rounded-lg font-semibold transition-colors"
          >
            Accept
          </button>
        </div>
      </div>
    </div>
  );
}
