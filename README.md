# Accredited Home Healthcare Directory (AHHD)

A search-first healthcare discovery platform connecting families with accredited home care, home health care, and hospice providers.

## Features

### Core Platform
- **Search-First Design**: Location-based search by city, ZIP code, or state
- **Structured Directory**: Organized provider listings with accreditation verification
- **Responsive Design**: Optimized for desktop, tablet, and mobile devices
- **Accessibility-Focused**: Large text, high contrast, AAA standards for older users

### Database Architecture
- **Complete Schema**: Organizations, locations, services, accreditations, reviews
- **Image Management**: Flexible optimization system based on image purpose
- **Keep-Alive System**: Automated hourly database pings via pg_cron
- **Review Workflow**: Quality control for imported data before publication
- **Search Analytics**: Track user search behavior and zero-result queries

### Admin Features
- **CMS Dashboard**: Manage listings, organizations, and content
- **Review Queue**: Flag and resolve data quality issues
- **Image Upload**: Automatic optimization based on purpose (hero, logo, etc.)
- **Role-Based Access**: Super Admin, Admin, Reviewer, Content Editor, Analytics Viewer

## Design System

### Color Palette
- **Primary Blue**: `#5BB4E5` (from logo) - trust, clarity, healthcare
- **Navy**: `#1A3B5C` - structure, authority, navigation
- **Neutral Grays**: Background and text hierarchy
- **Success Green**: `#2F8F6B` - verification, reassurance
- **Warning/Error**: Standard alert colors

### Typography
- **Headings**: Montserrat (geometric, modern, professional)
- **Body**: Open Sans (highly readable, 18px base for accessibility)
- **Line Height**: 150% body, 120% headings

### Spacing
- 8px base grid system
- Generous whitespace for stressed users
- 48-72px between major sections

## Database Schema

### Core Tables
- `organizations` - Parent healthcare organizations
- `locations` - Physical service locations (main listing unit)
- `service_types` - Controlled vocabulary (Home Care, Home Health, Hospice, etc.)
- `location_service_types` - Many-to-many relationship
- `accreditation_records` - Historical accreditation tracking
- `verification_events` - Admin review and verification logs
- `images` - Centralized image storage with metadata
- `articles` - Educational content and resources
- `search_logs` - User search analytics

### Review & Import System
- `import_runs` - Scraper batch tracking
- `imported_records` - Raw scraped data staging
- `review_queue_items` - Flagged items needing attention

### Admin System
- `admin_roles` - Role definitions
- `admin_user_roles` - User-role assignments
- `admin_activity_logs` - Audit trail
- `keepalive_pings` - Database activity monitoring

## Image Optimization

Images are automatically optimized based on their purpose:

| Purpose | Max Size | Quality | Use Case |
|---------|----------|---------|----------|
| Hero | 1920x1080 | 90% | Homepage/section heroes |
| Thumbnail | 400x300 | 85% | Card previews |
| Logo | 500x500 | 90% | Organization logos |
| Provider Logo | 300x300 | 90% | Enhanced listings |
| Content | 1200x800 | 85% | Article images |
| Background | 1920x1080 | 80% | Background patterns |
| Icon | 200x200 | 90% | Small icons/badges |

## Pages & Routes

### Public Pages
- `/` - Homepage with search hero
- `/find-care` - Search results with filters
- `/provider/:id` - Provider detail page
- `/resources` - Educational resources
- `/caregiver-guide` - Caregiver support
- `/about` - Mission and transparency
- `/get-listed` - Provider acquisition

### Admin Pages
- `/admin` - Dashboard with stats
- `/admin/locations` - Manage listings
- `/admin/review-queue` - Data quality review
- `/admin/organizations` - Organization management
- `/admin/images` - Image library
- `/admin/content` - Articles and resources

## Environment Variables

Create a `.env` file with:

```env
VITE_SUPABASE_URL=your_supabase_url
VITE_SUPABASE_ANON_KEY=your_supabase_anon_key
```

## Getting Started

1. **Install Dependencies**
   ```bash
   npm install
   ```

2. **Configure Supabase**
   - Database migrations are already applied
   - Update `.env` with your Supabase credentials

3. **Run Development Server**
   ```bash
   npm run dev
   ```

4. **Build for Production**
   ```bash
   npm run build
   ```

## Database Setup Complete

All core database tables have been created via Supabase migrations:
- ✅ Organizations and locations schema
- ✅ Service types and relationships
- ✅ Accreditation tracking system
- ✅ Review and verification workflow
- ✅ Image management with metadata
- ✅ Content and search analytics
- ✅ Admin roles and permissions
- ✅ pg_cron keep-alive system (runs hourly)

## Keep-Alive System

The database includes an automated keep-alive system:
- Runs every hour via pg_cron
- Logs pings to `keepalive_pings` table
- Automatically cleans up logs older than 7 days
- No external services required

## Next Steps

1. **Populate Service Types** (already seeded with defaults)
2. **Create First Admin User** via Supabase Auth
3. **Import Provider Data** via Python scraper or manual entry
4. **Upload Site Images** through admin panel
5. **Create Educational Content** (articles, guides, FAQs)
6. **Test Search Functionality** with sample data

## Key Design Principles

1. **Search First**: Primary action is always provider search
2. **Accessible Under Stress**: Large text, clear hierarchy, calm design
3. **Consistent Structure**: Controlled vocabularies, validation rules
4. **Founder-Friendly**: Automated workflows, clear error prevention
5. **Trust & Transparency**: Clear disclaimers, data source visibility

## Technologies

- **Frontend**: React 18, TypeScript, Tailwind CSS
- **Backend**: Supabase (PostgreSQL, Auth, Storage)
- **Build**: Vite
- **Fonts**: Montserrat (headings), Open Sans (body)
- **Icons**: Lucide React

## Browser Support

- Modern browsers (Chrome, Firefox, Safari, Edge)
- Mobile-optimized responsive design
- Keyboard navigation support
- Screen reader compatible

## License

All rights reserved. Accredited Home Healthcare Directory.
