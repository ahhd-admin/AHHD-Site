import { createClient } from '@supabase/supabase-js';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
  throw new Error('Missing Supabase environment variables');
}

export const supabase = createClient(supabaseUrl, supabaseAnonKey);

// Image optimization settings based on purpose
export const IMAGE_OPTIMIZATION_SETTINGS = {
  hero: {
    maxWidth: 1920,
    maxHeight: 1080,
    quality: 90,
    description: 'Large hero images for homepage and major sections'
  },
  thumbnail: {
    maxWidth: 400,
    maxHeight: 300,
    quality: 85,
    description: 'Small preview images for cards and lists'
  },
  logo: {
    maxWidth: 500,
    maxHeight: 500,
    quality: 90,
    description: 'Organization logos, preserved quality'
  },
  provider_logo: {
    maxWidth: 300,
    maxHeight: 300,
    quality: 90,
    description: 'Provider logos for enhanced listings'
  },
  content: {
    maxWidth: 1200,
    maxHeight: 800,
    quality: 85,
    description: 'Standard content images in articles'
  },
  background: {
    maxWidth: 1920,
    maxHeight: 1080,
    quality: 80,
    description: 'Background images and patterns'
  },
  icon: {
    maxWidth: 200,
    maxHeight: 200,
    quality: 90,
    description: 'Small icons and badges'
  }
} as const;

export type ImagePurpose = keyof typeof IMAGE_OPTIMIZATION_SETTINGS;

// Helper to get optimization settings for an image purpose
export function getImageOptimizationSettings(purpose: ImagePurpose) {
  return IMAGE_OPTIMIZATION_SETTINGS[purpose];
}
