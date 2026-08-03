// index.html's <link rel="canonical"> is a static tag pointing at the
// homepage -- fine for the homepage itself, but since every route in
// this SPA serves that same index.html, every OTHER page (every one of
// 6,000+ provider pages included) was also declaring the homepage as
// its canonical URL. That actively tells search engines "don't index
// this page separately, it's a duplicate of the homepage" -- the exact
// opposite of what the sitemap/structured data work is trying to
// achieve. Confirmed live via a real provider page before this existed.
const SITE_URL = 'https://www.accreditedhomehealthcare.directory';

/** Points the canonical tag at `path` (e.g. "/provider/some-slug-123").
 * Call from a page's own effect once it knows its real path; resets to
 * the homepage on unmount so a client-side navigation away doesn't leave
 * a stale canonical pointing at the page just left. */
export function setCanonicalUrl(path: string): () => void {
  const link = document.querySelector<HTMLLinkElement>('link[rel="canonical"]');
  const previous = link?.href;
  if (link) link.href = `${SITE_URL}${path}`;
  return () => {
    if (link && previous) link.href = previous;
  };
}
