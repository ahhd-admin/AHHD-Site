import { useEffect } from 'react';
import Router from './Router';
import CookieConsent from './components/CookieConsent';
import { initAnalyticsIfConsented } from './lib/analytics';

function App() {
  // Covers a RETURNING visitor who already accepted on a previous visit
  // -- CookieConsent's own handleChoice covers accepting during the
  // current visit, but that only fires once, the first time someone
  // sees the banner.
  useEffect(() => {
    initAnalyticsIfConsented();
  }, []);

  return (
    <>
      {/* Visually hidden until focused (Tab from page load) -- lets a
          keyboard/screen-reader user jump past the header nav straight to
          the page's main content instead of tabbing through it every
          single page load. Target #main-content is set on each page's
          own <main>. */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-[10000] focus:bg-white focus:text-navy-800 focus:px-4 focus:py-2 focus:rounded-lg focus:shadow-lg focus:outline-none focus:ring-2 focus:ring-primary-600"
      >
        Skip to main content
      </a>
      <Router />
      <CookieConsent />
    </>
  );
}

export default App;
