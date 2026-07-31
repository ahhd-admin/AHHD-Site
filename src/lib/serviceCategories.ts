export const SERVICE_CATEGORIES = {
  'home-based': {
    label: 'Home-Based Services',
    tag: 'Home-Based Services',
    services: ['home-care', 'home-health-care', 'hospice', 'home-infusion-therapy', 'palliative-care']
  },
  'facility-based': {
    label: 'Care Facilities',
    tag: 'Care Facilities',
    services: ['assisted-living', 'medical-clinic', 'ambulatory-care', 'behavioral-health', 'renal-dialysis', 'sleep', 'dentistry']
  },
  'retail-pharmacy': {
    label: 'Retail & Pharmacy',
    tag: 'Retail & Pharmacy',
    services: ['community-retail', 'pharmacy', 'dmepos']
  }
};

export const ALL_SERVICES: Record<string, string> = {
  'home-care': 'Home Care',
  'home-health-care': 'Home Health',
  'hospice': 'Hospice',
  'home-infusion-therapy': 'Home Infusion Therapy',
  'palliative-care': 'Palliative Care',
  'assisted-living': 'Assisted Living',
  'medical-clinic': 'Medical Clinic',
  'ambulatory-care': 'Ambulatory Care',
  'behavioral-health': 'Behavioral Health',
  'renal-dialysis': 'Renal Dialysis',
  'sleep': 'Sleep',
  'dentistry': 'Dentistry',
  'community-retail': 'Community Retail',
  'pharmacy': 'Pharmacy',
  'dmepos': 'DMEPOS'
};

export function getCategoryForService(serviceSlug: string): string | null {
  for (const [, category] of Object.entries(SERVICE_CATEGORIES)) {
    if (category.services.includes(serviceSlug)) {
      return category.tag;
    }
  }
  return null;
}
