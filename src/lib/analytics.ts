// Google Analytics, gated on cookie consent. Deliberately never imports
// or loads gtag.js unless the visitor has actually clicked "Accept" --
// "Decline" needs to mean the tracking script never runs at all, not
// just that the banner disappears.
//
// VITE_GA_MEASUREMENT_ID isn't set yet (waiting on a real GA4 property).
// Everything here is a safe no-op until it is -- set the env var and the
// site starts tracking on the next accepted visit, no other code changes
// needed.

import { getCookieConsent } from '../components/CookieConsent';

const MEASUREMENT_ID = import.meta.env.VITE_GA_MEASUREMENT_ID as string | undefined;

declare global {
  interface Window {
    dataLayer?: unknown[];
    gtag?: (...args: unknown[]) => void;
  }
}

let loaded = false;

function injectGtagScript(measurementId: string) {
  if (loaded) return;
  loaded = true;

  window.dataLayer = window.dataLayer || [];
  window.gtag = function gtag(...args: unknown[]) {
    window.dataLayer!.push(args);
  };
  window.gtag('js', new Date());
  window.gtag('config', measurementId);

  const script = document.createElement('script');
  script.async = true;
  script.src = `https://www.googletagmanager.com/gtag/js?id=${measurementId}`;
  document.head.appendChild(script);
}

/** Call once at startup, and again right after a visitor accepts -- a
 * no-op both times unless consent is actually 'accepted' and a real
 * measurement ID is configured. */
export function initAnalyticsIfConsented(): void {
  if (!MEASUREMENT_ID) return;
  if (getCookieConsent() !== 'accepted') return;
  injectGtagScript(MEASUREMENT_ID);
}

/** Fires a custom GA event -- silently does nothing if analytics hasn't
 * loaded (declined, or no measurement ID configured yet), so call sites
 * (a provider-website click, a search submit) never need their own
 * consent check. */
export function trackEvent(name: string, params?: Record<string, unknown>): void {
  if (!loaded || !window.gtag) return;
  window.gtag('event', name, params);
}
