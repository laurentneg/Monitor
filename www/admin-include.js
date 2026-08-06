/**
 * Charge le fragment admin partagé (admin-panel.html) dans un conteneur.
 * Utilisé par index.html (page admin) et monitor.html (#admin).
 *
 * Usage HTML :
 *   <div id="adminPanelRoot" data-admin-always="true"></div>
 *   <script src="admin-include.js"></script>
 *
 * data-admin-always="true" : affiche les sections immédiatement (page index).
 * Sinon : sections restent cachées jusqu'à #admin (monitor.js).
 */
(function (global) {
    const FRAGMENT_URL = '/admin-panel.html';

    async function mount(rootSelector) {
        const selector = rootSelector
            || (document.body && document.body.dataset.adminRoot)
            || '#adminPanelRoot';
        const root = document.querySelector(selector);
        if (!root) {
            return false;
        }
        if (root.dataset.adminLoaded === '1') {
            return true;
        }

        const response = await fetch(FRAGMENT_URL, { cache: 'no-store', credentials: 'same-origin' });
        if (!response.ok) {
            throw new Error('admin-panel.html HTTP ' + response.status);
        }
        root.innerHTML = await response.text();
        root.dataset.adminLoaded = '1';

        if (root.dataset.adminAlways === 'true') {
            root.querySelectorAll('#adminPulseAudioSection, #adminReloadSection').forEach((el) => {
                el.style.display = 'block';
            });
        }

        document.dispatchEvent(new CustomEvent('admin-panel-ready', { detail: { root } }));
        return true;
    }

    global.AdminPanel = {
        mount,
        FRAGMENT_URL
    };
})(window);
