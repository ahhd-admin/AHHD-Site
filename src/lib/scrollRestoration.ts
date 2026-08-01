// Provider links are plain <a href> navigations (a real page load, not
// client-side routing), so hitting back reloads the home page from
// scratch and re-fetches search results asynchronously. The browser's own
// scroll restoration fires before that fetch resolves, while the page is
// still short, so it lands near the bottom of the not-yet-repopulated
// page instead of where the visitor actually was. Call
// saveHomeScrollPosition() right before navigating away to a provider
// page, and restore it manually once results have actually re-rendered.
export const HOME_SCROLL_STORAGE_KEY = 'ahhd:homeScrollY';

export function saveHomeScrollPosition() {
  sessionStorage.setItem(HOME_SCROLL_STORAGE_KEY, String(window.scrollY));
}
