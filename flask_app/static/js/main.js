// INVOLEXIS Dashboard - Global Scripts
document.addEventListener('DOMContentLoaded', () => {
    console.log('INVOLEXIS Dashboard Initialized');
    
    // Auto-dismiss alerts after 3 seconds
    const alerts = document.querySelectorAll('.alert');
    alerts.forEach(alert => {
        setTimeout(() => {
            alert.style.opacity = '0';
            setTimeout(() => alert.remove(), 500);
        }, 3000);
    });
});
