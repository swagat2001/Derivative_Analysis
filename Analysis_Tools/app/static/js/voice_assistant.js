/**
 * ======================================================================
 * GOLDMINE VOICE ASSISTANT v3.0 - FULLY DYNAMIC EDITION
 * Complete voice control with 100% database-driven stock validation
 *
 * ✨ NEW in v3.0:
 * - Zero hardcoded stocks - all data from database
 * - Dynamic alias loading from backend API
 * - Automatic validation against available stocks
 * - Real-time sync with database changes
 *
 * Features:
 * - Dynamic stock list from database (/api/voice/stock-aliases)
 * - Context-aware commands based on current page
 * - Real-time page context from API
 * - Fuzzy matching for stock names
 * - All commands responsive to page state
 * - 120+ voice commands across all pages
 * ======================================================================
 */

class GoldmineVoiceAssistant {
  constructor() {
    // Check browser support
    this.supported = 'webkitSpeechRecognition' in window || 'SpeechRecognition' in window;
    this.speechSynthesisSupported = 'speechSynthesis' in window;

    if (!this.supported) {
      console.warn('[Voice] Speech recognition not supported in this browser');
      this.showNotSupportedUI();
      return;
    }

    // Initialize speech recognition
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    this.recognition = new SpeechRecognition();
    this.recognition.continuous = false;
    this.recognition.interimResults = true;
    this.recognition.lang = 'en-IN';
    this.recognition.maxAlternatives = 3;

    // State
    this.isListening = false;
    this.isProcessing = false;
    this.lastTranscript = '';
    this.currentPage = this.detectCurrentPage();

    // Dynamic data from API
    this.availableStocks = [];
    this.stockAliases = {};
    this.pageContext = null;

    // DOM elements
    this.button = null;
    this.modal = null;
    this.overlay = null;

    // Initialize
    this.createUI();
    this.bindEvents();
    this.bindKeyboardShortcuts();

    // Load dynamic data
    this.loadAvailableStocks();
    this.loadPageContext();

    // Show welcome
    this.showWelcomeBadge();

    // Watch for page changes
    this.observePageChanges();

    console.log(`[Voice] Initialized on page: ${this.currentPage}`);
  }

  /**
   * Load available stocks and aliases from database via API
   * This makes the voice assistant fully dynamic - no hardcoded stocks!
   */
  async loadAvailableStocks() {
    try {
      // Load stock aliases from backend (already filtered for stocks in database)
      const response = await fetch('/api/voice/stock-aliases');
      const data = await response.json();

      if (data.success) {
        this.availableStocks = data.available_stocks || [];
        this.stockAliases = data.aliases || {};
        console.log(`[Voice] Loaded ${data.stock_count} stocks and ${Object.keys(this.stockAliases).length} aliases from database`);
        console.log('[Voice] Sample aliases:', Object.entries(this.stockAliases).slice(0, 5));
      } else {
        console.error('[Voice] Failed to load stock aliases:', data.error);
        // Fallback to empty state
        this.availableStocks = [];
        this.stockAliases = {};
      }
    } catch (error) {
      console.error('[Voice] Failed to load stocks:', error);
      this.availableStocks = [];
      this.stockAliases = {};
    }
  }

  /**
   * REMOVED: buildStockAliases() - No longer needed!
   * Stock aliases are now loaded dynamically from the backend via /api/voice/stock-aliases
   * This ensures the voice assistant always has up-to-date stock information from the database
   */

  /**
   * Load page context from API
   */
  async loadPageContext() {
    try {
      const response = await fetch(`/api/voice/page-context/${this.currentPage}`);
      const data = await response.json();

      if (data.success) {
        this.pageContext = data;
        this.updateHintsFromContext();
        console.log(`[Voice] Loaded context for page: ${this.currentPage}`);
      }
    } catch (error) {
      console.error('[Voice] Failed to load page context:', error);
    }
  }

  /**
   * Update hints display from loaded context
   */
  updateHintsFromContext() {
    const hintsEl = document.getElementById('voiceHints');
    if (!hintsEl || !this.pageContext) return;

    const hints = this.pageContext.hints || [];
    hintsEl.innerHTML = hints.map(h => `<span class="voice-hint">${h}</span>`).join('');
  }

  /**
   * Detect which page user is currently on
   */
  detectCurrentPage() {
    const path = window.location.pathname;

    if (path === '/' || path === '') return 'home';
    if (path === '/dashboard' || path === '/dashboard/') return 'dashboard';
    if (path.startsWith('/stock/')) return 'stock_detail';
    if (path === '/scanner/' || path === '/screener') return 'screener_landing';
    if (path.includes('top-gainers-losers')) return 'top_gainers_losers';
    if (path.includes('signal-analysis')) return 'signal_analysis';
    if (path.includes('futures-oi')) return 'futures_oi';
    if (path.includes('technical')) return 'technical_screener';
    if (path.includes('/scanner/index') || path.includes('/scanner/banknifty') ||
      path.includes('/scanner/high-oi') || path.includes('/scanner/iv-spike')) return 'index_screener';
    if (path === '/login') return 'login';
    if (path === '/signup') return 'signup';

    return 'unknown';
  }

  /**
   * Get current stock ticker if on stock detail page
   */
  getCurrentTicker() {
    const path = window.location.pathname;
    const match = path.match(/\/stock\/([A-Za-z0-9-]+)/);
    return match ? match[1].toUpperCase() : null;
  }

  /**
   * Observe page changes
   */
  observePageChanges() {
    window.addEventListener('popstate', () => {
      this.currentPage = this.detectCurrentPage();
      this.loadPageContext();
      this.updateContextDisplay();
    });

    let lastUrl = location.href;
    new MutationObserver(() => {
      if (location.href !== lastUrl) {
        lastUrl = location.href;
        this.currentPage = this.detectCurrentPage();
        this.loadPageContext();
        this.updateContextDisplay();
      }
    }).observe(document, { subtree: true, childList: true });
  }

  /**
   * Show UI for unsupported browsers
   */
  showNotSupportedUI() {
    const btn = document.createElement('button');
    btn.className = 'voice-assistant-btn voice-disabled';
    btn.setAttribute('title', 'Voice not supported. Use Chrome or Edge.');
    btn.innerHTML = `
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"></path>
        <path d="M19 10v2a7 7 0 0 1-14 0v-2"></path>
        <line x1="1" y1="1" x2="23" y2="23" stroke="#dc2626" stroke-width="3"></line>
      </svg>
    `;
    document.body.appendChild(btn);
  }

  /**
   * Create UI elements
   */
  createUI() {
    // Floating button
    this.button = document.createElement('button');
    this.button.className = 'voice-assistant-btn';
    this.button.id = 'voiceAssistantBtn';
    this.button.setAttribute('aria-label', 'Voice Assistant');
    this.button.innerHTML = `
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"></path>
        <path d="M19 10v2a7 7 0 0 1-14 0v-2"></path>
        <line x1="12" y1="19" x2="12" y2="23"></line>
        <line x1="8" y1="23" x2="16" y2="23"></line>
      </svg>
    `;
    document.body.appendChild(this.button);

    // Shortcut hint
    const hint = document.createElement('div');
    hint.className = 'voice-shortcut-hint';
    hint.innerHTML = 'Press <kbd>Space</kbd>';
    document.body.appendChild(hint);

    // Modal
    this.overlay = document.createElement('div');
    this.overlay.className = 'voice-modal-overlay';
    this.overlay.innerHTML = `
      <div class="voice-modal">
        <button class="voice-close-btn" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <line x1="18" y1="6" x2="6" y2="18"></line>
            <line x1="6" y1="6" x2="18" y2="18"></line>
          </svg>
        </button>

        <div class="voice-animation">
          <div class="voice-ripple"></div>
          <div class="voice-ripple"></div>
          <div class="voice-ripple"></div>
          <div class="voice-circle">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"></path>
              <path d="M19 10v2a7 7 0 0 1-14 0v-2"></path>
              <line x1="12" y1="19" x2="12" y2="23"></line>
            </svg>
          </div>
        </div>

        <div class="sound-wave">
          ${Array(9).fill('<div class="sound-bar"></div>').join('')}
        </div>

        <div class="voice-status" id="voiceStatus">Listening...</div>
        <div class="voice-transcript placeholder" id="voiceTranscript">Say a command...</div>
        <div class="voice-response" id="voiceResponse"></div>

        <div class="voice-context" id="voiceContext">
          <span class="context-badge">📍 <span id="currentPageName">Page</span></span>
          <span class="stock-count-badge" id="stockCountBadge"></span>
        </div>

        <div class="voice-hints" id="voiceHints"></div>
      </div>
    `;
    document.body.appendChild(this.overlay);

    this.modal = this.overlay.querySelector('.voice-modal');

    // Toast
    this.toast = document.createElement('div');
    this.toast.className = 'voice-toast';
    document.body.appendChild(this.toast);

    this.updateContextDisplay();
  }

  /**
   * Update context display
   */
  updateContextDisplay() {
    const pageNames = {
      'home': 'Home',
      'dashboard': 'Dashboard',
      'stock_detail': `Stock: ${this.getCurrentTicker() || 'Detail'}`,
      'screener_landing': 'Scanners',
      'top_gainers_losers': 'Top Gainers & Losers',
      'signal_analysis': 'Signal Analysis',
      'futures_oi': 'Futures OI',
      'technical_screener': 'Technical Scanner',
      'index_screener': 'Index Scanner',
      'login': 'Login',
      'signup': 'Sign Up',
      'unknown': 'Page'
    };

    const el = document.getElementById('currentPageName');
    if (el) el.textContent = pageNames[this.currentPage] || 'Page';

    const countEl = document.getElementById('stockCountBadge');
    if (countEl && this.availableStocks.length > 0) {
      countEl.textContent = `📊 ${this.availableStocks.length} stocks`;
    }
  }

  /**
   * Bind events
   */
  bindEvents() {
    this.button.addEventListener('click', () => this.toggle());
    this.overlay.querySelector('.voice-close-btn').addEventListener('click', () => this.close());
    this.overlay.addEventListener('click', (e) => {
      if (e.target === this.overlay) this.close();
    });

    this.recognition.onstart = () => {
      this.isListening = true;
      this.updateUI('listening');
    };

    this.recognition.onend = () => {
      this.isListening = false;
      if (!this.isProcessing) this.updateUI('idle');
    };

    this.recognition.onresult = (event) => {
      let interim = '';
      let final = '';

      for (let i = event.resultIndex; i < event.results.length; i++) {
        const transcript = event.results[i][0].transcript;
        if (event.results[i].isFinal) {
          final += transcript;
        } else {
          interim += transcript;
        }
      }

      if (interim) this.updateTranscript(interim, false);
      if (final) {
        this.lastTranscript = final;
        this.updateTranscript(final, true);
        this.processCommand(final);
      }
    };

    this.recognition.onerror = (event) => {
      this.isListening = false;
      const errors = {
        'no-speech': 'No speech detected. Try again.',
        'audio-capture': 'No microphone found.',
        'not-allowed': 'Microphone access denied.',
        'network': 'Network error.',
        'service-not-allowed': 'Use HTTPS or localhost for voice.'
      };
      this.showResponse(errors[event.error] || `Error: ${event.error}`, 'error');
      this.updateUI('idle');
    };
  }

  /**
   * Bind keyboard shortcuts
   */
  bindKeyboardShortcuts() {
    document.addEventListener('keydown', (e) => {
      const isTyping = ['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName);

      if (e.code === 'Space' && !isTyping && !e.ctrlKey && !e.altKey) {
        e.preventDefault();
        this.toggle();
      }

      if (e.code === 'Escape' && this.overlay.classList.contains('active')) {
        this.close();
      }

      if (e.code === 'KeyV' && e.ctrlKey && e.shiftKey) {
        e.preventDefault();
        this.toggle();
      }
    });
  }

  toggle() {
    this.isListening ? this.stop() : this.start();
  }

  start() {
    this.currentPage = this.detectCurrentPage();
    this.updateContextDisplay();
    this.loadPageContext();

    this.overlay.classList.add('active');
    this.updateUI('listening');
    this.updateTranscript('', false);
    this.showResponse('');

    try {
      this.recognition.start();
    } catch (e) {
      this.recognition.stop();
      setTimeout(() => this.recognition.start(), 100);
    }
  }

  stop() {
    try { this.recognition.stop(); } catch (e) { }
    this.updateUI('idle');
  }

  close() {
    this.stop();
    this.overlay.classList.remove('active');
  }

  updateUI(state) {
    this.button.classList.remove('listening', 'processing');
    this.modal.classList.remove('listening', 'processing');

    const status = document.getElementById('voiceStatus');

    if (state === 'listening') {
      this.button.classList.add('listening');
      this.modal.classList.add('listening');
      if (status) status.textContent = 'Listening...';
    } else if (state === 'processing') {
      this.button.classList.add('processing');
      this.modal.classList.add('processing');
      if (status) status.textContent = 'Processing...';
    } else {
      if (status) status.textContent = 'Tap mic or press Space';
    }
  }

  updateTranscript(text, isFinal) {
    const el = document.getElementById('voiceTranscript');
    if (!el) return;
    el.textContent = text || 'Say a command...';
    el.classList.toggle('placeholder', !text);
  }

  showResponse(text, type = '') {
    const el = document.getElementById('voiceResponse');
    if (!el) return;
    el.textContent = text;
    el.className = 'voice-response' + (type ? ` ${type}` : '');
  }

  showToast(message, type = '') {
    this.toast.textContent = message;
    this.toast.className = 'voice-toast' + (type ? ` ${type}` : '') + ' show';
    setTimeout(() => this.toast.classList.remove('show'), 3000);
  }

  showWelcomeBadge() {
    const badge = document.createElement('div');
    badge.className = 'voice-supported-badge';
    badge.innerHTML = '🎤 Voice ready! Press <kbd>Space</kbd>';
    document.body.appendChild(badge);

    setTimeout(() => badge.classList.add('show'), 500);
    setTimeout(() => {
      badge.classList.remove('show');
      setTimeout(() => badge.remove(), 300);
    }, 4000);
  }

  speak(text, callback = null) {
    if (!this.speechSynthesisSupported) {
      if (callback) callback();
      return;
    }

    window.speechSynthesis.cancel();

    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = 'en-IN';
    utterance.rate = 1.0;

    const voices = window.speechSynthesis.getVoices();
    const voice = voices.find(v => v.lang.includes('en') && v.name.includes('Google')) || voices.find(v => v.lang.includes('en'));
    if (voice) utterance.voice = voice;

    if (callback) utterance.onend = callback;

    window.speechSynthesis.speak(utterance);
  }

  /**
   * Process voice command
   */
  processCommand(command) {
    this.isProcessing = true;
    this.updateUI('processing');

    const cmd = command.toLowerCase().trim();
    console.log(`[Voice] Processing: "${cmd}" on ${this.currentPage}`);

    let response = this.parseCommand(cmd);

    this.showResponse(response.text, response.success ? 'success' : 'error');

    if (response.speech) {
      this.speak(response.speech, () => {
        if (response.action) {
          setTimeout(() => {
            response.action();
            if (response.closeAfter !== false) this.close();
          }, 300);
        }
      });
    } else if (response.action) {
      setTimeout(() => {
        response.action();
        if (response.closeAfter !== false) this.close();
      }, 1000);
    }

    this.isProcessing = false;
    this.updateUI('idle');
  }

  /**
   * Main command parser
   */
  parseCommand(cmd) {
    // Remove wake words
    cmd = cmd.replace(/^(hey |hi |hello |ok |okay )?(goldmine|gold mine)\s*/i, '').trim();

    if (!cmd) {
      return { text: 'How can I help?', speech: 'Yes?', success: true, closeAfter: false };
    }

    // Try global commands first
    let result = this.parseGlobalCommands(cmd);
    if (result) return result;

    // Then page-specific commands
    result = this.parsePageCommands(cmd);
    if (result) return result;

    // Unknown
    return {
      text: `Didn't understand "${cmd}". Say "help" for commands.`,
      speech: 'Sorry, try saying help.',
      success: false
    };
  }

  /**
   * Global commands (work on any page)
   */
  parseGlobalCommands(cmd) {
    // Navigation
    if (this.match(cmd, ['go to home', 'go home', 'home page', 'home'])) {
      return this.navigate('/', 'home');
    }

    if (this.match(cmd, ['go to dashboard', 'dashboard', 'open dashboard'])) {
      return this.navigate('/dashboard', 'dashboard');
    }

    if (this.match(cmd, ['go to screeners', 'screeners', 'open screeners', 'screener hub', 'go to scanners', 'scanners', 'open scanners', 'scanner hub'])) {
      return this.navigate('/scanner/', 'scanners');
    }

    if (this.match(cmd, ['top gainers', 'gainers losers', 'top losers'])) {
      return this.navigate('/scanner/top-gainers-losers', 'top gainers');
    }

    if (this.match(cmd, ['signal analysis', 'signals', 'trading signals'])) {
      return this.navigate('/scanner/signal-analysis', 'signal analysis');
    }

    if (this.match(cmd, ['futures oi', 'futures', 'open interest'])) {
      return this.navigate('/scanner/futures-oi', 'futures OI');
    }

    if (this.match(cmd, ['technical screener', 'technical', 'technical analysis', 'technical scanner'])) {
      return this.navigate('/scanner/technical-indicators', 'technical scanner');
    }

    if (this.match(cmd, ['nifty 50', 'nifty fifty', 'nifty'])) {
      return this.navigate('/scanner/index', 'Nifty 50');
    }

    if (this.match(cmd, ['bank nifty', 'banknifty'])) {
      return this.navigate('/scanner/banknifty', 'Bank Nifty');
    }

    if (this.match(cmd, ['high oi', 'high open interest', 'oi buildup'])) {
      return this.navigate('/scanner/high-oi', 'high OI');
    }

    if (this.match(cmd, ['iv spike', 'volatility spike'])) {
      return this.navigate('/scanner/iv-spike', 'IV spike');
    }

    // Dynamic stock search - check against database
    const searchResult = this.parseStockSearch(cmd);
    if (searchResult) return searchResult;

    // Price query
    const priceResult = this.parsePriceQuery(cmd);
    if (priceResult) return priceResult;

    // Market summary
    if (this.match(cmd, ['market summary', 'market update', 'how is market', 'market status'])) {
      this.fetchMarketSummary();
      return { text: 'Getting market summary...', speech: 'Checking market', success: true, closeAfter: false };
    }

    // Scroll commands
    if (this.match(cmd, ['scroll down', 'go down', 'down'])) {
      return { text: 'Scrolling down', speech: 'Down', success: true, action: () => window.scrollBy({ top: 500, behavior: 'smooth' }) };
    }

    if (this.match(cmd, ['scroll up', 'go up', 'up'])) {
      return { text: 'Scrolling up', speech: 'Up', success: true, action: () => window.scrollBy({ top: -500, behavior: 'smooth' }) };
    }

    if (this.match(cmd, ['go to top', 'top', 'scroll to top'])) {
      return { text: 'Going to top', speech: 'Top', success: true, action: () => window.scrollTo({ top: 0, behavior: 'smooth' }) };
    }

    if (this.match(cmd, ['go to bottom', 'bottom'])) {
      return { text: 'Going to bottom', speech: 'Bottom', success: true, action: () => window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' }) };
    }

    // Utility
    if (this.match(cmd, ['refresh', 'reload'])) {
      return { text: 'Refreshing...', speech: 'Refreshing', success: true, action: () => location.reload() };
    }

    if (this.match(cmd, ['go back', 'back'])) {
      return { text: 'Going back', speech: 'Back', success: true, action: () => history.back() };
    }

    if (this.match(cmd, ['logout', 'log out', 'sign out'])) {
      return { text: 'Logging out...', speech: 'Goodbye', success: true, action: () => location.href = '/logout' };
    }

    // Help
    if (this.match(cmd, ['help', 'commands', 'what can you do'])) {
      return this.getHelpResponse();
    }

    // Greetings
    if (this.match(cmd, ['hello', 'hi', 'hey', 'good morning', 'good afternoon', 'good evening'])) {
      const hour = new Date().getHours();
      const greeting = hour < 12 ? 'Good morning' : hour < 17 ? 'Good afternoon' : 'Good evening';
      return { text: `${greeting}! How can I help?`, speech: `${greeting}! How can I help?`, success: true, closeAfter: false };
    }

    // Stop/Cancel
    if (this.match(cmd, ['stop', 'cancel', 'never mind'])) {
      return { text: 'Okay', speech: 'Okay', success: true, action: () => this.close() };
    }

    return null;
  }

  /**
   * Parse stock search command - DYNAMIC from database
   */
  parseStockSearch(cmd) {
    // Patterns for stock search
    const patterns = [
      /^(?:search|find|show|open|look up)\s+(?:stock\s+)?(.+)$/i,
      /^(?:go to|goto)\s+(.+)\s+stock$/i,
      /^(.+)\s+stock$/i
    ];

    for (const pattern of patterns) {
      const match = cmd.match(pattern);
      if (match) {
        const stockQuery = match[1].trim().toLowerCase();
        const ticker = this.resolveStockName(stockQuery);

        if (ticker) {
          return this.navigate(`/stock/${ticker}`, ticker);
        } else {
          return {
            text: `"${match[1]}" not found in database`,
            speech: `Sorry, ${match[1]} is not available in the database`,
            success: false
          };
        }
      }
    }

    // Check if the command itself is a stock name
    const directTicker = this.resolveStockName(cmd);
    if (directTicker && cmd.split(' ').length <= 2) {
      return this.navigate(`/stock/${directTicker}`, directTicker);
    }

    return null;
  }

  /**
   * Parse price query
   */
  parsePriceQuery(cmd) {
    const patterns = [
      /(?:what is|what's|tell me|get)\s+(?:the\s+)?(?:price of\s+)?(.+?)(?:\s+price)?$/i,
      /(?:price of|price for)\s+(.+)$/i
    ];

    for (const pattern of patterns) {
      const match = cmd.match(pattern);
      if (match && !this.match(cmd, ['go', 'open', 'navigate'])) {
        const stockQuery = match[1].trim().toLowerCase();
        const ticker = this.resolveStockName(stockQuery);

        if (ticker) {
          this.fetchStockData(ticker);
          return { text: `Getting ${ticker} price...`, speech: `Checking ${ticker}`, success: true, closeAfter: false };
        }
      }
    }

    return null;
  }

  /**
   * Resolve stock name to ticker - checks database
   */
  resolveStockName(query) {
    const lower = query.toLowerCase().trim();

    // Check aliases first
    if (this.stockAliases[lower]) {
      return this.stockAliases[lower];
    }

    // Check direct match in available stocks
    const upper = query.toUpperCase();
    if (this.availableStocks.includes(upper)) {
      return upper;
    }

    // Fuzzy match against available stocks
    for (const stock of this.availableStocks) {
      if (this.fuzzyMatch(lower, stock.toLowerCase())) {
        return stock;
      }
    }

    // Check if partial match
    for (const stock of this.availableStocks) {
      if (stock.toLowerCase().includes(lower) || lower.includes(stock.toLowerCase())) {
        return stock;
      }
    }

    return null; // Not found in database
  }

  /**
   * Page-specific commands
   */
  parsePageCommands(cmd) {
    switch (this.currentPage) {
      case 'dashboard': return this.parseDashboardCommands(cmd);
      case 'stock_detail': return this.parseStockDetailCommands(cmd);
      case 'screener_landing': return this.parseScreenerCommands(cmd);
      case 'top_gainers_losers': return this.parseTopGainersCommands(cmd);
      case 'signal_analysis': return this.parseSignalCommands(cmd);
      case 'futures_oi': return this.parseFuturesCommands(cmd);
      case 'technical_screener': return this.parseTechnicalCommands(cmd);
      case 'index_screener': return this.parseIndexCommands(cmd);
    }
    return null;
  }

  parseDashboardCommands(cmd) {
    if (this.match(cmd, ['filter itm', 'itm', 'in the money'])) {
      return this.clickElement('[data-mtype="ITM"], #mtypeITM, input[value="ITM"]', 'Filtering ITM');
    }
    if (this.match(cmd, ['filter otm', 'otm', 'out of the money'])) {
      return this.clickElement('[data-mtype="OTM"], #mtypeOTM, input[value="OTM"]', 'Filtering OTM');
    }
    if (this.match(cmd, ['show all', 'total', 'clear filter'])) {
      return this.clickElement('[data-mtype="TOTAL"], #mtypeTOTAL, input[value="TOTAL"]', 'Showing all');
    }
    if (this.match(cmd, ['export excel', 'export', 'download excel'])) {
      return this.clickElement('[data-export], .export-btn, #exportBtn', 'Exporting Excel');
    }
    if (this.match(cmd, ['next page', 'next'])) {
      return this.clickElement('.paginate_button.next:not(.disabled)', 'Next page');
    }
    if (this.match(cmd, ['previous page', 'prev', 'back'])) {
      return this.clickElement('.paginate_button.previous:not(.disabled)', 'Previous page');
    }
    if (this.match(cmd, ['change date', 'select date'])) {
      return this.focusElement('#dateSelect, select[name="date"]', 'Select a date');
    }
    return null;
  }

  parseStockDetailCommands(cmd) {
    if (this.match(cmd, ['next expiry', 'next month'])) {
      return this.clickElement('.expiry-next, [data-expiry="next"]', 'Next expiry');
    }
    if (this.match(cmd, ['previous expiry', 'prev expiry'])) {
      return this.clickElement('.expiry-prev, [data-expiry="prev"]', 'Previous expiry');
    }
    if (this.match(cmd, ['change expiry', 'select expiry'])) {
      return this.focusElement('#expirySelect, select[name="expiry"]', 'Select expiry');
    }
    if (this.match(cmd, ['show stats', 'statistics'])) {
      return this.scrollToElement('.stats-section, #statsSection', 'Showing stats');
    }
    if (this.match(cmd, ['show chart', 'oi chart'])) {
      return this.scrollToElement('.chart-section, #chartSection', 'Showing chart');
    }
    if (this.match(cmd, ['what is trend', 'trend', 'is it bullish'])) {
      const ticker = this.getCurrentTicker();
      if (ticker) {
        this.fetchStockData(ticker);
        return { text: `Checking ${ticker} trend...`, speech: `Checking trend`, success: true, closeAfter: false };
      }
    }
    if (this.match(cmd, ['export', 'download'])) {
      return this.clickElement('[data-export], .export-btn', 'Exporting data');
    }
    return null;
  }

  parseScreenerCommands(cmd) {
    if (this.match(cmd, ['filter bullish', 'bullish', 'bullish only'])) {
      return this.clickElement('[data-filter="bullish"], .filter-bullish', 'Filtering bullish');
    }
    if (this.match(cmd, ['filter bearish', 'bearish', 'bearish only'])) {
      return this.clickElement('[data-filter="bearish"], .filter-bearish', 'Filtering bearish');
    }
    if (this.match(cmd, ['show all', 'all', 'clear filter'])) {
      return this.clickElement('[data-filter="all"], .filter-all', 'Showing all');
    }
    if (this.match(cmd, ['derivative', 'derivative screeners'])) {
      return this.clickElement('[data-category="derivative"]', 'Derivative screeners');
    }
    if (this.match(cmd, ['technical', 'technical screeners'])) {
      return this.clickElement('[data-category="technical"]', 'Technical screeners');
    }
    if (this.match(cmd, ['back', 'back to list'])) {
      return this.clickElement('.back-btn, [data-action="back"]', 'Going back');
    }
    return null;
  }

  parseTopGainersCommands(cmd) {
    if (this.match(cmd, ['export pdf', 'download pdf', 'pdf'])) {
      return this.clickElement('#exportPdfBtn, [data-export-pdf]', 'Exporting PDF');
    }
    if (this.match(cmd, ['oi section', 'show oi'])) {
      return this.scrollToElement('#oiSection, .oi-section', 'OI section');
    }
    if (this.match(cmd, ['iv section', 'show iv'])) {
      return this.scrollToElement('#ivSection, .iv-section', 'IV section');
    }
    if (this.match(cmd, ['futures section', 'show futures'])) {
      return this.scrollToElement('#futuresSection, .futures-section', 'Futures section');
    }
    if (this.match(cmd, ['change date', 'select date'])) {
      return this.focusElement('#dateSelect', 'Select date');
    }
    return null;
  }

  parseSignalCommands(cmd) {
    if (this.match(cmd, ['filter bullish', 'bullish'])) {
      return this.clickElement('[data-signal="bullish"], .filter-bullish', 'Bullish signals');
    }
    if (this.match(cmd, ['filter bearish', 'bearish'])) {
      return this.clickElement('[data-signal="bearish"], .filter-bearish', 'Bearish signals');
    }
    if (this.match(cmd, ['show all', 'all'])) {
      return this.clickElement('[data-signal="all"], .filter-all', 'All signals');
    }
    if (this.match(cmd, ['export', 'download'])) {
      return this.clickElement('[data-export]', 'Exporting');
    }
    return null;
  }

  parseFuturesCommands(cmd) {
    if (this.match(cmd, ['current month', 'cme', 'near month'])) {
      return this.clickElement('[data-expiry="cme"], .tab-cme', 'Current month');
    }
    if (this.match(cmd, ['next month', 'nme'])) {
      return this.clickElement('[data-expiry="nme"], .tab-nme', 'Next month');
    }
    if (this.match(cmd, ['far month', 'fme'])) {
      return this.clickElement('[data-expiry="fme"], .tab-fme', 'Far month');
    }
    if (this.match(cmd, ['export', 'download'])) {
      return this.clickElement('[data-export]', 'Exporting');
    }
    return null;
  }

  parseTechnicalCommands(cmd) {
    if (this.match(cmd, ['golden crossover', 'golden cross'])) {
      return this.navigate('/scanner/technical-indicators/golden-crossover', 'golden crossover');
    }
    if (this.match(cmd, ['death crossover', 'death cross'])) {
      return this.navigate('/scanner/technical-indicators/death-crossover', 'death crossover');
    }
    if (this.match(cmd, ['rsi overbought', 'overbought'])) {
      return this.clickElement('[data-indicator="rsi-overbought"]', 'RSI overbought');
    }
    if (this.match(cmd, ['rsi oversold', 'oversold'])) {
      return this.clickElement('[data-indicator="rsi-oversold"]', 'RSI oversold');
    }
    if (this.match(cmd, ['macd bullish'])) {
      return this.clickElement('[data-indicator="macd-bullish"]', 'MACD bullish');
    }
    return null;
  }

  parseIndexCommands(cmd) {
    if (this.match(cmd, ['filter bullish', 'bullish'])) {
      return this.clickElement('[data-filter="bullish"]', 'Bullish stocks');
    }
    if (this.match(cmd, ['filter bearish', 'bearish'])) {
      return this.clickElement('[data-filter="bearish"]', 'Bearish stocks');
    }
    if (this.match(cmd, ['show all', 'all'])) {
      return this.clickElement('[data-filter="all"]', 'All stocks');
    }
    if (this.match(cmd, ['sort by price', 'sort price'])) {
      return this.clickElement('th[data-sort="price"]', 'Sorting by price');
    }
    if (this.match(cmd, ['sort by change', 'sort change'])) {
      return this.clickElement('th[data-sort="change"]', 'Sorting by change');
    }
    if (this.match(cmd, ['export', 'export csv', 'download'])) {
      return this.clickElement('[data-export-csv], [data-export]', 'Exporting');
    }
    if (this.match(cmd, ['back', 'go back'])) {
      return this.clickElement('.back-btn', 'Going back');
    }
    return null;
  }

  // Helper functions

  match(cmd, patterns) {
    return patterns.some(p => cmd === p || cmd.includes(p));
  }

  fuzzyMatch(a, b) {
    if (a === b) return true;
    if (Math.abs(a.length - b.length) > 2) return false;

    let diff = 0;
    const longer = a.length > b.length ? a : b;
    const shorter = a.length > b.length ? b : a;

    for (let i = 0; i < longer.length; i++) {
      if (shorter[i] !== longer[i]) diff++;
    }

    return diff <= 2;
  }

  navigate(url, name) {
    return {
      text: `Opening ${name}...`,
      speech: `Opening ${name}`,
      success: true,
      action: () => location.href = url
    };
  }

  clickElement(selectors, text) {
    for (const sel of selectors.split(', ')) {
      const el = document.querySelector(sel);
      if (el) {
        return {
          text: text,
          speech: text,
          success: true,
          action: () => el.click()
        };
      }
    }
    return { text: 'Not available on this page', speech: 'Not available here', success: false };
  }

  focusElement(selectors, text) {
    for (const sel of selectors.split(', ')) {
      const el = document.querySelector(sel);
      if (el) {
        return {
          text: text,
          speech: text,
          success: true,
          action: () => { el.focus(); if (el.tagName === 'SELECT') el.click(); },
          closeAfter: false
        };
      }
    }
    return { text: 'Not available', success: false };
  }

  scrollToElement(selectors, text) {
    for (const sel of selectors.split(', ')) {
      const el = document.querySelector(sel);
      if (el) {
        return {
          text: text,
          speech: text,
          success: true,
          action: () => el.scrollIntoView({ behavior: 'smooth', block: 'start' })
        };
      }
    }
    return { text: 'Section not found', success: false };
  }

  getHelpResponse() {
    const stockCount = this.availableStocks.length;
    const pageHints = this.pageContext?.hints || [];

    let helpText = `On this page: ${pageHints.join(', ')}. `;
    helpText += `Global: Go to dashboard, Search any of ${stockCount} stocks, Market summary, Scroll up/down.`;

    return {
      text: helpText,
      speech: `You can say: ${pageHints.slice(0, 3).join(', ')}. Or search any of ${stockCount} stocks.`,
      success: true,
      closeAfter: false
    };
  }

  async fetchStockData(ticker) {
    try {
      const res = await fetch(`/api/voice/stock/${ticker}`);
      const data = await res.json();

      this.showResponse(data.speech, data.success ? 'success' : 'error');
      this.speak(data.speech);
    } catch (e) {
      this.showResponse(`Error getting ${ticker} data`, 'error');
      this.speak(`Sorry, couldn't get ${ticker} data`);
    }
  }

  async fetchMarketSummary() {
    try {
      const res = await fetch('/api/voice/market-summary');
      const data = await res.json();

      this.showResponse(data.speech, data.success ? 'success' : 'error');
      this.speak(data.speech);
    } catch (e) {
      this.showResponse('Error getting market data', 'error');
      this.speak('Sorry, couldn\'t get market data');
    }
  }
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
  window.goldmineVoice = new GoldmineVoiceAssistant();
});

if ('speechSynthesis' in window) {
  window.speechSynthesis.onvoiceschanged = () => window.speechSynthesis.getVoices();
}
