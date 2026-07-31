import { useEffect, useState } from 'react';
import { loadGoogleMapsScript } from '../lib/googleMapsLoader';

export function useGoogleMaps() {
  const [isLoaded, setIsLoaded] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    loadGoogleMapsScript()
      .then(() => setIsLoaded(true))
      .catch((err) => setError(err));
  }, []);

  return { isLoaded, error };
}
