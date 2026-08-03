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
      <Router />
      <CookieConsent />
    </>
  );
}

export default App;
