export interface PlaceDetails {
  name: string;
  formatted_address: string;
  address_line_1: string;
  address_line_2?: string;
  city: string;
  state: string;
  postal_code: string;
  county?: string;
  latitude: number;
  longitude: number;
  phone?: string;
  website?: string;
  place_id: string;
}

export interface PlaceAutocompleteResult {
  place_id: string;
  description: string;
  structured_formatting: {
    main_text: string;
    secondary_text: string;
  };
}

let autocompleteService: google.maps.places.AutocompleteService | null = null;
let placesService: google.maps.places.PlacesService | null = null;

function getAutocompleteService(): google.maps.places.AutocompleteService {
  if (!autocompleteService && typeof google !== 'undefined' && google.maps && google.maps.places) {
    autocompleteService = new google.maps.places.AutocompleteService();
  }
  if (!autocompleteService) {
    throw new Error('Google Maps not loaded');
  }
  return autocompleteService;
}

function getPlacesService(): google.maps.places.PlacesService {
  if (!placesService && typeof google !== 'undefined' && google.maps && google.maps.places) {
    const div = document.createElement('div');
    placesService = new google.maps.places.PlacesService(div);
  }
  if (!placesService) {
    throw new Error('Google Maps not loaded');
  }
  return placesService;
}

export async function autocompletePlace(input: string): Promise<PlaceAutocompleteResult[]> {
  if (!input || input.length < 3) {
    return [];
  }

  try {
    const service = getAutocompleteService();

    return new Promise((resolve) => {
      service.getPlacePredictions(
        {
          input,
          componentRestrictions: { country: 'us' },
        },
        (predictions, status) => {
          if (status === google.maps.places.PlacesServiceStatus.OK && predictions) {
            const results = predictions.slice(0, 5).map((prediction) => ({
              place_id: prediction.place_id,
              description: prediction.description,
              structured_formatting: {
                main_text: prediction.structured_formatting.main_text,
                secondary_text: prediction.structured_formatting.secondary_text || '',
              },
            }));
            resolve(results);
          } else {
            resolve([]);
          }
        }
      );
    });
  } catch (error) {
    console.error('Error autocompleting place:', error);
    return [];
  }
}

export async function searchPlaces(query: string): Promise<any[]> {
  const apiKey = import.meta.env.VITE_GOOGLE_MAPS_API_KEY;

  try {
    const response = await fetch(
      `https://maps.googleapis.com/maps/api/place/textsearch/json?query=${encodeURIComponent(query)}&key=${apiKey}`
    );
    const data = await response.json();

    if (data.status === 'OK') {
      return data.results.slice(0, 5);
    }
    return [];
  } catch (error) {
    console.error('Error searching places:', error);
    return [];
  }
}

export async function getPlaceDetails(placeId: string): Promise<PlaceDetails | null> {
  try {
    const service = getPlacesService();

    return new Promise((resolve) => {
      service.getDetails(
        {
          placeId,
          fields: ['name', 'formatted_address', 'address_components', 'geometry', 'formatted_phone_number', 'website'],
        },
        (place, status) => {
          if (status === google.maps.places.PlacesServiceStatus.OK && place) {
            const addressComponents = place.address_components || [];

            let streetNumber = '';
            let route = '';
            let city = '';
            let state = '';
            let postalCode = '';
            let county = '';

            addressComponents.forEach((component) => {
              const types = component.types;
              if (types.includes('street_number')) {
                streetNumber = component.long_name;
              } else if (types.includes('route')) {
                route = component.long_name;
              } else if (types.includes('locality')) {
                city = component.long_name;
              } else if (types.includes('administrative_area_level_1')) {
                state = component.short_name;
              } else if (types.includes('postal_code')) {
                postalCode = component.long_name;
              } else if (types.includes('administrative_area_level_2')) {
                county = component.long_name.replace(/\s+County$/i, '');
              }
            });

            const address_line_1 = `${streetNumber} ${route}`.trim();

            resolve({
              name: place.name || '',
              formatted_address: place.formatted_address || '',
              address_line_1,
              city,
              state,
              postal_code: postalCode,
              county,
              latitude: place.geometry?.location?.lat() || 0,
              longitude: place.geometry?.location?.lng() || 0,
              phone: place.formatted_phone_number,
              website: place.website,
              place_id: placeId,
            });
          } else {
            resolve(null);
          }
        }
      );
    });
  } catch (error) {
    console.error('Error fetching place details:', error);
    return null;
  }
}

export async function geocodeAddress(address: string): Promise<{ lat: number; lng: number; county?: string } | null> {
  const apiKey = import.meta.env.VITE_GOOGLE_MAPS_API_KEY;

  try {
    const response = await fetch(
      `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${apiKey}`
    );
    const data = await response.json();

    if (data.status === 'OK' && data.results && data.results.length > 0) {
      const result = data.results[0];
      const location = result.geometry.location;

      let county = '';
      const addressComponents = result.address_components || [];
      addressComponents.forEach((component: any) => {
        if (component.types.includes('administrative_area_level_2')) {
          county = component.long_name.replace(/\s+County$/i, '');
        }
      });

      return {
        lat: location.lat,
        lng: location.lng,
        county: county || undefined
      };
    }
    return null;
  } catch (error) {
    console.error('Error geocoding address:', error);
    return null;
  }
}

export async function findBusinessAtAddress(address: string): Promise<string | null> {
  const apiKey = import.meta.env.VITE_GOOGLE_MAPS_API_KEY;

  try {
    const response = await fetch(
      `https://maps.googleapis.com/maps/api/place/textsearch/json?query=${encodeURIComponent(address)}&key=${apiKey}`
    );
    const data = await response.json();

    if (data.status === 'OK' && data.results && data.results.length > 0) {
      const result = data.results[0];
      if (result.types.includes('establishment') || result.types.includes('health')) {
        return result.name;
      }
    }
    return null;
  } catch (error) {
    console.error('Error finding business at address:', error);
    return null;
  }
}
