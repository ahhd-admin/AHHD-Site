import { ImagePurpose, getImageOptimizationSettings } from './supabase';

/**
 * Image optimization utility for flexible, iterative optimization
 * based on image purpose (hero, thumbnail, logo, etc.)
 */

export interface ImageOptimizationResult {
  optimizedFile?: File;
  width: number;
  height: number;
  fileSize: number;
  optimizationApplied: boolean;
  purpose: ImagePurpose;
  maxWidth: number;
  maxHeight: number;
  quality: number;
}

/**
 * Optimize an image based on its intended purpose
 * Automatically applies appropriate size constraints and quality settings
 */
export async function optimizeImageForPurpose(
  file: File,
  purpose: ImagePurpose
): Promise<ImageOptimizationResult> {
  const settings = getImageOptimizationSettings(purpose);

  return new Promise((resolve, reject) => {
    const reader = new FileReader();

    reader.onload = (e) => {
      const img = new Image();

      img.onload = () => {
        const originalWidth = img.width;
        const originalHeight = img.height;

        const needsResize =
          originalWidth > settings.maxWidth ||
          originalHeight > settings.maxHeight;

        if (!needsResize) {
          resolve({
            optimizedFile: undefined,
            width: originalWidth,
            height: originalHeight,
            fileSize: file.size,
            optimizationApplied: false,
            purpose,
            maxWidth: settings.maxWidth,
            maxHeight: settings.maxHeight,
            quality: settings.quality
          });
          return;
        }

        let newWidth = originalWidth;
        let newHeight = originalHeight;

        if (originalWidth > settings.maxWidth) {
          newWidth = settings.maxWidth;
          newHeight = (originalHeight * settings.maxWidth) / originalWidth;
        }

        if (newHeight > settings.maxHeight) {
          newHeight = settings.maxHeight;
          newWidth = (originalWidth * settings.maxHeight) / originalHeight;
        }

        const canvas = document.createElement('canvas');
        canvas.width = newWidth;
        canvas.height = newHeight;

        const ctx = canvas.getContext('2d');
        if (!ctx) {
          reject(new Error('Could not get canvas context'));
          return;
        }

        ctx.drawImage(img, 0, 0, newWidth, newHeight);

        canvas.toBlob(
          (blob) => {
            if (!blob) {
              reject(new Error('Could not create optimized image'));
              return;
            }

            const optimizedFile = new File(
              [blob],
              file.name,
              { type: file.type }
            );

            resolve({
              optimizedFile,
              width: Math.round(newWidth),
              height: Math.round(newHeight),
              fileSize: blob.size,
              optimizationApplied: true,
              purpose,
              maxWidth: settings.maxWidth,
              maxHeight: settings.maxHeight,
              quality: settings.quality
            });
          },
          file.type,
          settings.quality / 100
        );
      };

      img.onerror = () => {
        reject(new Error('Could not load image'));
      };

      img.src = e.target?.result as string;
    };

    reader.onerror = () => {
      reject(new Error('Could not read file'));
    };

    reader.readAsDataURL(file);
  });
}

export function getImagePreviewUrl(file: File): string {
  return URL.createObjectURL(file);
}

export function validateImageFile(file: File): { valid: boolean; error?: string } {
  const validTypes = ['image/jpeg', 'image/jpg', 'image/png', 'image/webp', 'image/gif'];

  if (!validTypes.includes(file.type)) {
    return {
      valid: false,
      error: 'Invalid file type. Please upload a JPEG, PNG, WebP, or GIF image.'
    };
  }

  const maxSize = 10 * 1024 * 1024;
  if (file.size > maxSize) {
    return {
      valid: false,
      error: 'File size too large. Maximum size is 10MB.'
    };
  }

  return { valid: true };
}
