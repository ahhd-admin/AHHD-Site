let googleMapsPromise: Promise<void> | null = null;

export function loadGoogleMapsScript(): Promise<void> {
  if (googleMapsPromise) {
    return googleMapsPromise;
  }

  if (typeof google !== 'undefined' && google.maps && google.maps.places) {
    return Promise.resolve();
  }

  googleMapsPromise = new Promise((resolve, reject) => {
    const apiKey = import.meta.env.VITE_GOOGLE_MAPS_API_KEY;

    if (!apiKey) {
      reject(new Error('Google Maps API key not found'));
      return;
    }

    if (document.querySelector('script[src*="maps.googleapis.com"]')) {
      const checkGoogle = setInterval(() => {
        if (typeof google !== 'undefined' && google.maps && google.maps.places) {
          clearInterval(checkGoogle);
          resolve();
        }
      }, 100);
      return;
    }

    const script = document.createElement('script');
    script.src = `https://maps.googleapis.com/maps/api/js?key=${apiKey}&libraries=places`;
    script.async = true;
    script.defer = true;

    script.onload = () => {
      const checkGoogle = setInterval(() => {
        if (typeof google !== 'undefined' && google.maps && google.maps.places) {
          clearInterval(checkGoogle);
          resolve();
        }
      }, 100);
    };

    script.onerror = () => {
      reject(new Error('Failed to load Google Maps script'));
    };

    document.head.appendChild(script);
  });

  return googleMapsPromise;
}

export function isGoogleMapsLoaded(): boolean {
  return typeof google !== 'undefined' && google.maps && google.maps.places;
}
