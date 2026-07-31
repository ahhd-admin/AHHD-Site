// Filters are scoped to what's actually in the database today: Home Care,
// Home Health, and Hospice (the MVP pull). The other categories/services
// below (Care Facilities, Retail & Pharmacy, Home Infusion Therapy,
// Palliative Care, etc.) were part of Bolt's originally-planned full scope,
// but nothing in Supabase is intentionally tagged with them yet -- keep
// them commented out for reference and re-enable a category once real,
// intentional data using those tags actually exists.

export const SERVICE_CATEGORIES = {
  'home-based': {
    label: 'Home-Based Services',
    tag: 'Home-Based Services',
    services: ['home-care', 'home-health-care', 'hospice']
  }
  // 'facility-based': {
  //   label: 'Care Facilities',
  //   tag: 'Care Facilities',
  //   services: ['assisted-living', 'medical-clinic', 'ambulatory-care', 'behavioral-health', 'renal-dialysis', 'sleep', 'dentistry']
  // },
  // 'retail-pharmacy': {
  //   label: 'Retail & Pharmacy',
  //   tag: 'Retail & Pharmacy',
  //   services: ['community-retail', 'pharmacy', 'dmepos']
  // }
};

export const ALL_SERVICES: Record<string, string> = {
  'home-care': 'Home Care',
  'home-health-care': 'Home Health',
  'hospice': 'Hospice'
  // 'home-infusion-therapy': 'Home Infusion Therapy',
  // 'palliative-care': 'Palliative Care',
  // 'assisted-living': 'Assisted Living',
  // 'medical-clinic': 'Medical Clinic',
  // 'ambulatory-care': 'Ambulatory Care',
  // 'behavioral-health': 'Behavioral Health',
  // 'renal-dialysis': 'Renal Dialysis',
  // 'sleep': 'Sleep',
  // 'dentistry': 'Dentistry',
  // 'community-retail': 'Community Retail',
  // 'pharmacy': 'Pharmacy',
  // 'dmepos': 'DMEPOS'
};

export function getCategoryForService(serviceSlug: string): string | null {
  for (const [, category] of Object.entries(SERVICE_CATEGORIES)) {
    if (category.services.includes(serviceSlug)) {
      return category.tag;
    }
  }
  return null;
}
