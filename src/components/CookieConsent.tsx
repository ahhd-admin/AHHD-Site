import { useEffect, useState } from 'react';
import { Cookie } from 'lucide-react';
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
    // aria-live="polite" so a screen reader user actually hears that this
    // appeared -- confirmed via an accessibility audit that role="region"
    // alone is silent on appearance (a landmark is only announced if a
    // user happens to navigate into it, not proactively). Deliberately
    // polite, not role="alert"/assertive -- declining doesn't lose any
    // data or block anything, so it doesn't need to interrupt whatever
    // the visitor is already doing, and focus is intentionally left where
    // it was rather than forced onto the banner.
    <div
      className="fixed bottom-0 inset-x-0 z-50 bg-navy-800 text-white border-t border-navy-600 shadow-lg"
      role="region"
      aria-live="polite"
      aria-label="Cookie notice"
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex flex-col sm:flex-row sm:items-center gap-4">
        {/* Icon badge matches TrustStats' treatment on the home page --
            same visual language, and gives this row a real anchor instead
            of a paragraph floating alone. Tightened copy (cut "so we can
            improve AHHD" and "to track you", both implied) fits two even
            lines within max-w-xl instead of the previous version's one
            long line + one short one. */}
        <div className="flex items-center gap-3 flex-1 max-w-2xl">
          <div className="w-10 h-10 bg-navy-700 rounded-full flex items-center justify-center flex-shrink-0">
            <Cookie className="w-5 h-5 text-primary-200" aria-hidden="true" />
          </div>
          <p className="text-sm text-primary-100 leading-relaxed">
            We use Google Analytics to see which searches and pages are most useful, not for ads or cross-site tracking. See our{' '}
            {/* Color set directly on the <a>, not inherited -- index.css's
                global `a { text-primary-600 }` rule overrides any parent
                color otherwise (see the documented gotcha in
                DESIGN-SYSTEM-COLOR-PALETTE.md). */}
            <a href="/privacy" className="text-white underline hover:no-underline font-medium">
              Privacy Policy
            </a>.
          </p>
        </div>
        {/* Sized to match Search/"My Location" (h-11/44px, same tap-target
            height established earlier for mobile) rather than shrinking to
            fit "Decline"/"Accept" alone -- min-w keeps the text itself
            compact/centered instead of stretching across the wider button. */}
        <div className="flex gap-3 flex-shrink-0 sm:ml-auto">
          <button
            onClick={() => handleChoice('declined')}
            className="border-2 border-white text-white hover:bg-white hover:text-navy-800 text-sm h-11 min-w-[120px] px-4 rounded-lg font-semibold transition-colors"
          >
            Decline
          </button>
          <button
            onClick={() => handleChoice('accepted')}
            className="bg-white text-neutral-900 hover:bg-neutral-100 text-sm h-11 min-w-[120px] px-4 rounded-lg font-semibold transition-colors"
          >
            Accept
          </button>
        </div>
      </div>
    </div>
  );
}
