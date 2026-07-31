import { useEffect, useState } from 'react';
import HomePage from './pages/HomePage';
import ProviderDetailPage from './pages/ProviderDetailPage';
import AdminDashboard from './pages/AdminDashboard';
import AdminLogin from './pages/AdminLogin';
import ManageListings from './pages/admin/ManageListings';
import ReviewQueue from './pages/admin/ReviewQueue';
import ManageOrganizations from './pages/admin/ManageOrganizations';
import ManageImages from './pages/admin/ManageImages';
import ManageContent from './pages/admin/ManageContent';
import SearchAnalytics from './pages/admin/SearchAnalytics';
import ProtectedRoute from './components/ProtectedRoute';
import { useAuth } from './contexts/AuthContext';

export default function Router() {
  const [currentPath, setCurrentPath] = useState(window.location.pathname);
  const { user } = useAuth();

  useEffect(() => {
    const handleLocationChange = () => {
      setCurrentPath(window.location.pathname);
    };

    window.addEventListener('popstate', handleLocationChange);

    const originalPushState = window.history.pushState;
    window.history.pushState = function (...args) {
      originalPushState.apply(window.history, args);
      handleLocationChange();
    };

    return () => {
      window.removeEventListener('popstate', handleLocationChange);
      window.history.pushState = originalPushState;
    };
  }, []);

  if (currentPath === '/' || currentPath === '/find-care') {
    return <HomePage />;
  }

  if (currentPath.startsWith('/provider/')) {
    return <ProviderDetailPage />;
  }

  if (currentPath === '/admin' && !currentPath.includes('/admin/')) {
    if (user) {
      window.history.pushState({}, '', '/admin/dashboard');
      return <ProtectedRoute><AdminDashboard /></ProtectedRoute>;
    }
    return <AdminLogin />;
  }

  if (currentPath === '/admin/dashboard') {
    return <ProtectedRoute><AdminDashboard /></ProtectedRoute>;
  }

  if (currentPath === '/admin/locations') {
    return <ProtectedRoute><ManageListings /></ProtectedRoute>;
  }

  if (currentPath === '/admin/review-queue') {
    return <ProtectedRoute><ReviewQueue /></ProtectedRoute>;
  }

  if (currentPath === '/admin/organizations') {
    return <ProtectedRoute><ManageOrganizations /></ProtectedRoute>;
  }

  if (currentPath === '/admin/images') {
    return <ProtectedRoute><ManageImages /></ProtectedRoute>;
  }

  if (currentPath === '/admin/content') {
    return <ProtectedRoute><ManageContent /></ProtectedRoute>;
  }

  if (currentPath === '/admin/search-logs') {
    return <ProtectedRoute><SearchAnalytics /></ProtectedRoute>;
  }

  if (currentPath.startsWith('/admin')) {
    return <ProtectedRoute><AdminDashboard /></ProtectedRoute>;
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-neutral-50">
      <div className="text-center">
        <h1 className="text-4xl font-heading font-bold text-navy-800 mb-4">Page Not Found</h1>
        <p className="text-neutral-600 mb-6">The page you're looking for doesn't exist.</p>
        <a href="/" className="btn-primary">
          Return Home
        </a>
      </div>
    </div>
  );
}
